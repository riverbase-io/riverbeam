-- Development only (plan 027 D5): drops every beam object in the schema
-- `search_path` names, keeping diesel's ledger so the revert can record itself.
-- On a database shared with fluvius-beam this deletes Python's tables too, so
-- it refuses unless the session opts in first:
--
--     SET riverbeam.allow_baseline_down = on;
--
-- It also forgets the schema in the pre-diesel ledger (`riverbeam_meta`), so
-- the next migrate rebuilds the schema instead of bridging an empty one.
DO $$
DECLARE
    r record;
BEGIN
    IF coalesce(current_setting('riverbeam.allow_baseline_down', true), '') <> 'on' THEN
        RAISE EXCEPTION 'refusing to drop the beam objects of schema %: '
            'SET riverbeam.allow_baseline_down = on first (development only)', current_schema();
    END IF;
    FOR r IN SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema() AND c.relkind IN ('r', 'p', 'v', 'm')
               AND c.relname <> '__diesel_schema_migrations' LOOP
        EXECUTE format(CASE r.relkind
                           WHEN 'v' THEN 'DROP VIEW IF EXISTS %I.%I CASCADE'
                           WHEN 'm' THEN 'DROP MATERIALIZED VIEW IF EXISTS %I.%I CASCADE'
                           ELSE 'DROP TABLE IF EXISTS %I.%I CASCADE'
                       END, current_schema(), r.relname);
    END LOOP;
    FOR r IN SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = current_schema() AND c.relkind = 'S' LOOP
        EXECUTE format('DROP SEQUENCE IF EXISTS %I.%I CASCADE', current_schema(), r.relname);
    END LOOP;
    FOR r IN SELECT t.typname FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
             WHERE n.nspname = current_schema() AND t.typtype IN ('e', 'd') LOOP
        EXECUTE format('DROP TYPE IF EXISTS %I.%I CASCADE', current_schema(), r.typname);
    END LOOP;
    IF to_regclass('riverbeam_meta.schema_migrations') IS NOT NULL THEN
        DELETE FROM riverbeam_meta.schema_migrations WHERE schema_name = current_schema();
    END IF;
END $$;
