# Recorded schemas (plan 005 Phase 02, plans 027 and 028)

`riverbeam_baseline.sql` is the beam schema: what migrations 0002 through 0012 built, applied by the pre-diesel runner and dumped (normalized like `python_ref_beam.sql`) before they were combined (plan 027). `crates/beam_store/migrations/0012_baseline/up.sql` is this schema as creation DDL, naming no schema (plan 028). `scripts/schema_diff.py` (`just schema-diff`) applies the baseline to fresh databases (the default schema, another schema name, and after `down.sql`) and diffs the result against it.

`python_ref_beam.sql` is fluvius-beam **rc8**'s `ref_beam` schema (`2.0.0-rc8`, `/private/abx/abx-agent/lib/fluvius-beam`), dumped by `scripts/dump_python_schema.py` (`just schema-dump`; re-dumped from rc8 in plan 020). The beam schema started from it; `crates/beam_store/src/rows.rs` and `crates/beam_execution/src/status.rs` test their enum labels against it.

Never edit these by hand.

Conventions they fix, which the Rust store follows:

- Enum columns hold Python enum **member names** (`ACTIVE`, `IN_PROGRESS`), not values. `agentstatus` also has lower-case labels added by Python's `add_revision_columns`; Python never writes them.
- Every table has the Fluvius columns `_id`, `_created`, `_updated`, `_creator`, `_updater`, `_deleted` (soft delete), `_etag` and `_realm`. Reads skip `_deleted IS NOT NULL`; updates set `_updated` and a new random `_etag`.
- Row-level security is **not** part of the schema. `crates/beam_store/sql/rls.sql` is Python's optional operator DDL.
- riverbeam's migration ledger is diesel's, `<schema>.__diesel_schema_migrations`, inside the beam schema; `schema_diff.py` leaves it out of every comparison. The pre-diesel runner's ledger, `riverbeam_meta.schema_migrations`, is only read to bridge a schema it migrated.
