//! The Postgres schema that holds the beam tables: `BEAM_SQL_SCHEMA`, else
//! `riverbeam`. Tests isolate by database instead (`testdb`, with the
//! `testing` feature) and always use `ref_beam` (`testdb::schema`).

use std::fmt;

/// The beam schema when `BEAM_SQL_SCHEMA` is not set.
pub const DEFAULT_SCHEMA: &str = "riverbeam";

/// The environment variable that names the beam schema.
pub const SCHEMA_ENV: &str = "BEAM_SQL_SCHEMA";

/// Postgres reserved key words (SQL Key Words appendix, "reserved" column).
const RESERVED: &[&str] = &[
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "placing",
    "primary",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "similar",
    "some",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// The beam schema identifier, spliced into SQL text: [`SchemaName::from_env`]
/// outside tests.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SchemaName(String);

impl SchemaName {
    /// Validate a schema name.
    ///
    /// # Errors
    /// Returns a store error if the name is empty, too long, or not a plain lower-case identifier.
    pub fn new(name: &str) -> crate::store::StoreResult<Self> {
        let ok = !name.is_empty()
            && name.len() <= 63
            && name
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
            && name
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        // Shared or system schemas could hold other applications' tables, and
        // reserved words would need quoting.
        let reserved = name == "public"
            || name == "information_schema"
            || name == "riverbeam_meta"
            || name.starts_with("pg_")
            || RESERVED.contains(&name);
        if ok && !reserved {
            Ok(Self(name.to_owned()))
        } else {
            Err(crate::BEM_110.with_data(format!("invalid schema {name:?}")))
        }
    }

    /// `BEAM_SQL_SCHEMA` (trimmed), else [`DEFAULT_SCHEMA`].
    ///
    /// # Errors
    /// `BEM-110` when the variable is not a plain lower-case identifier.
    pub fn from_env() -> crate::store::StoreResult<Self> {
        match beam_env::string(SCHEMA_ENV) {
            Some(name) => Self::new(&name),
            None => Ok(Self::default()),
        }
    }

    /// `url` for a pool whose queries leave the beam tables unqualified (the
    /// domain query resources): `search_path` is `public`, then this schema.
    /// `public` stays first so riverbase's own migrations land where they
    /// always did. A URL that already sets `options` is left alone.
    #[must_use]
    pub fn pool_url(&self, url: &str) -> String {
        if url.contains("options=") {
            return url.to_owned();
        }
        let sep = if url.contains('?') { '&' } else { '?' };
        format!("{url}{sep}options=-c%20search_path%3Dpublic%2C{}", self.0)
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for SchemaName {
    fn default() -> Self {
        Self(DEFAULT_SCHEMA.to_owned())
    }
}

impl fmt::Display for SchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{SchemaName, DEFAULT_SCHEMA};

    #[test]
    fn validates_identifiers() {
        assert!(SchemaName::new(DEFAULT_SCHEMA).is_ok());
        assert_eq!(SchemaName::default().as_str(), "riverbeam");
        assert!(SchemaName::new("ref_beam").is_ok());
        assert!(SchemaName::new("_x1").is_ok());
        for bad in [
            "",
            "Ref",
            "1x",
            "a-b",
            "a;b",
            "a b",
            &"x".repeat(64),
            "public",
            "pg_catalog",
            "information_schema",
            "riverbeam_meta",
            "user",
            "order",
        ] {
            assert!(SchemaName::new(bad).is_err(), "{bad:?}");
        }
        assert!(SchemaName::new("riverbeam").is_ok());
    }

    #[test]
    fn pool_url_sets_the_search_path() {
        let schema = SchemaName::new("riverbeam").unwrap();
        assert_eq!(
            schema.pool_url("postgres://u@h:5432/db"),
            "postgres://u@h:5432/db?options=-c%20search_path%3Dpublic%2Criverbeam"
        );
        assert_eq!(
            schema.pool_url("postgres://u@h/db?sslmode=disable"),
            "postgres://u@h/db?sslmode=disable&options=-c%20search_path%3Dpublic%2Criverbeam"
        );
        let own = "postgres://u@h/db?options=-c%20search_path%3Dx";
        assert_eq!(schema.pool_url(own), own);
    }
}
