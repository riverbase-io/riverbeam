//! Result rows several repositories read from SQL.

use diesel::sql_types::{BigInt, Bool, Jsonb};
use diesel::QueryableByName;
use serde_json::Value;

/// A `count(*) AS n`.
#[derive(QueryableByName)]
pub(crate) struct Counted {
    #[diesel(sql_type = BigInt)]
    pub(crate) n: i64,
}

/// An `EXISTS (…) AS found`.
#[derive(QueryableByName)]
pub(crate) struct Found {
    #[diesel(sql_type = Bool)]
    pub(crate) found: bool,
}

/// One JSONB column, `row`.
#[derive(QueryableByName)]
pub(crate) struct JsonRow {
    #[diesel(sql_type = Jsonb)]
    pub(crate) row: Value,
}
