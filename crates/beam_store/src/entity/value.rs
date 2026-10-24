//! Reading a write's JSON payload: one typed value per column,
//! and enum labels as SQL literals. Used by the generated `upsert_<table>`,
//! and by the typed bulk updates of `beam_store` and the other beam crates
//! (`enum_sql`, `now`, `new_etag`; plans 014, 015).

use chrono::{DateTime, Utc};
use diesel::dsl::sql;
use diesel::expression::{SqlLiteral, TypedExpressionType};
use riverbase_core::datastore::DataResult;
use serde_json::Value;
use uuid::Uuid;

/// A column's value as JSON carries it: what `row_to_json` writes (UUIDs and
/// timestamps as strings).
pub(crate) trait JsonColumn: Sized {
    /// `None` when `value` is not this type.
    fn parse(value: &Value) -> Option<Self>;
}

impl JsonColumn for Uuid {
    fn parse(value: &Value) -> Option<Self> {
        value.as_str().and_then(|s| Uuid::parse_str(s.trim()).ok())
    }
}

impl JsonColumn for String {
    fn parse(value: &Value) -> Option<Self> {
        value.as_str().map(str::to_owned)
    }
}

impl JsonColumn for i32 {
    fn parse(value: &Value) -> Option<Self> {
        value.as_i64().and_then(|n| i32::try_from(n).ok())
    }
}

impl JsonColumn for f64 {
    fn parse(value: &Value) -> Option<Self> {
        value.as_f64()
    }
}

impl JsonColumn for bool {
    fn parse(value: &Value) -> Option<Self> {
        value.as_bool()
    }
}

/// Any JSON, `null` included (a `jsonb` column that is `NOT NULL` stores it).
impl JsonColumn for Value {
    fn parse(value: &Value) -> Option<Self> {
        Some(value.clone())
    }
}

impl JsonColumn for DateTime<Utc> {
    fn parse(value: &Value) -> Option<Self> {
        value
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|t| t.with_timezone(&Utc))
    }
}

impl JsonColumn for Vec<Option<String>> {
    fn parse(value: &Value) -> Option<Self> {
        value
            .as_array()?
            .iter()
            .map(|item| match item {
                Value::Null => Some(None),
                Value::String(s) => Some(Some(s.clone())),
                _ => None,
            })
            .collect()
    }
}

/// A nullable column: JSON `null` is SQL `NULL`.
impl<T: JsonColumn> JsonColumn for Option<T> {
    fn parse(value: &Value) -> Option<Self> {
        if value.is_null() {
            Some(None)
        } else {
            T::parse(value).map(Some)
        }
    }
}

/// `column` of `data`: `None` when the payload does not name it.
///
/// # Errors
/// `BEM-146` when `data` is not an object, or the value is not the column's
/// type (`null` for a column that is `NOT NULL`).
pub(crate) fn take<T: JsonColumn>(data: &Value, column: &str) -> DataResult<Option<T>> {
    let Some(object) = data.as_object() else {
        return Err(crate::BEM_146.with_data("the payload is not a JSON object"));
    };
    object
        .get(column)
        .map(|value| T::parse(value).ok_or_else(|| crate::BEM_146.with_data(column.to_owned())))
        .transpose()
}

/// `label` as a literal of the enum type `ty` (`NULL` for `None`), checked
/// against the type's `labels`. The column is `Text` in the Diesel schema,
/// so the bound value would be `text`, which Postgres does not cast to an
/// enum on its own; the type name resolves through the pool's `search_path`.
///
/// # Errors
/// `BEM-146` for a label `ty` does not have.
pub fn enum_sql<ST: TypedExpressionType>(
    label: Option<&str>,
    column: &str,
    ty: &str,
    labels: &[&str],
) -> DataResult<SqlLiteral<ST>> {
    let Some(label) = label else {
        return Ok(sql("NULL"));
    };
    Ok(sql(&format!("{}::{ty}", quote(label, column, labels)?)))
}

/// `items` as a literal array of the enum type `ty` (`NULL` for `None`); see
/// [`enum_sql`].
///
/// # Errors
/// `BEM-146` for a label `ty` does not have.
pub(crate) fn enum_array_sql<ST: TypedExpressionType>(
    items: Option<&[Option<String>]>,
    column: &str,
    ty: &str,
    labels: &[&str],
) -> DataResult<SqlLiteral<ST>> {
    let Some(items) = items else {
        return Ok(sql("NULL"));
    };
    let items = items
        .iter()
        .map(|item| match item {
            Some(label) => quote(label, column, labels),
            None => Ok("NULL".to_owned()),
        })
        .collect::<DataResult<Vec<_>>>()?;
    Ok(sql(&format!("ARRAY[{}]::{ty}[]", items.join(", "))))
}

/// The database's `now()`, for an audit column a typed update sets (the
/// transaction's start time, as the SQL `TOUCH` rule wrote it).
pub fn now<ST: TypedExpressionType>() -> SqlLiteral<ST> {
    sql("now()")
}

/// A new `_etag` per row (`gen_random_uuid()`), for a typed bulk update.
pub fn new_etag<ST: TypedExpressionType>() -> SqlLiteral<ST> {
    sql("gen_random_uuid()")
}

/// `label` quoted, when it is one of `labels`.
fn quote(label: &str, column: &str, labels: &[&str]) -> DataResult<String> {
    if !labels.contains(&label) {
        return Err(crate::BEM_146.with_data(format!("{column}: unknown label {label:?}")));
    }
    Ok(format!("'{}'", label.replace('\'', "''")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel::sql_types::{Nullable, Text};
    use serde_json::json;

    #[test]
    fn absent_null_and_wrong_types() {
        let data = json!({"a": "x", "n": null, "i": 3});
        assert_eq!(take::<String>(&data, "a").unwrap(), Some("x".to_owned()));
        assert_eq!(take::<String>(&data, "missing").unwrap(), None);
        assert_eq!(take::<Option<String>>(&data, "n").unwrap(), Some(None));
        assert!(take::<String>(&data, "n").is_err(), "null for NOT NULL");
        assert!(take::<String>(&data, "i").is_err(), "a number is not text");
        assert_eq!(take::<i32>(&data, "i").unwrap(), Some(3));
        assert!(take::<String>(&json!([1]), "a").is_err(), "not an object");
    }

    #[test]
    fn timestamps_and_arrays() {
        let data = json!({"t": "2026-10-20T01:02:03+07:00", "xs": ["A", null], "bad": [1]});
        let t = take::<DateTime<Utc>>(&data, "t").unwrap().unwrap();
        assert_eq!(t.to_rfc3339(), "2026-10-19T18:02:03+00:00");
        assert_eq!(
            take::<Vec<Option<String>>>(&data, "xs").unwrap(),
            Some(vec![Some("A".to_owned()), None])
        );
        assert!(take::<Vec<Option<String>>>(&data, "bad").is_err());
    }

    #[test]
    fn enum_labels_are_checked() {
        let labels = ["ACTIVE", "INACTIVE"];
        assert!(enum_sql::<Text>(Some("ACTIVE"), "status", "agentstatus", &labels).is_ok());
        assert!(enum_sql::<Text>(Some("x'); DROP"), "status", "agentstatus", &labels).is_err());
        assert!(enum_sql::<Nullable<Text>>(None, "status", "agentstatus", &labels).is_ok());
        assert_eq!(quote("ACTIVE", "status", &labels).unwrap(), "'ACTIVE'");
        let items = [Some("ACTIVE".to_owned()), Some("GONE".to_owned())];
        assert!(enum_array_sql::<Nullable<Text>>(Some(&items), "m", "modality", &labels).is_err());
    }
}
