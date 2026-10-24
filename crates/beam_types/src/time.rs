//! Lenient datetime parsing, matching what Pydantic accepts for `datetime` fields.
//!
//! Python may send an RFC 3339 string with an offset, a naive ISO datetime
//! (Pydantic dumps naive values without an offset), a date only, or unix
//! seconds. Naive values are read as UTC. Serialization stays RFC 3339 (`...Z`),
//! which Pydantic reads back.

use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde::{Deserialize, Deserializer};
use serde_json::Value;

fn parse(value: &Value) -> Result<DateTime<Utc>, String> {
    match value {
        Value::String(text) => parse_str(text),
        Value::Number(n) => {
            let secs = n.as_f64().ok_or_else(|| format!("invalid timestamp {n}"))?;
            #[allow(clippy::cast_possible_truncation)]
            let millis = (secs * 1000.0).round() as i64;
            Utc.timestamp_millis_opt(millis)
                .single()
                .ok_or_else(|| format!("timestamp out of range: {n}"))
        }
        other => Err(format!("expected a datetime, got {other}")),
    }
}

fn parse_str(text: &str) -> Result<DateTime<Utc>, String> {
    let text = text.trim();
    if let Ok(dt) = DateTime::parse_from_rfc3339(text) {
        return Ok(dt.with_timezone(&Utc));
    }
    for format in [
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%dT%H:%M",
    ] {
        if let Ok(naive) = NaiveDateTime::parse_from_str(text, format) {
            return Ok(Utc.from_utc_datetime(&naive));
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(text, "%Y-%m-%d") {
        if let Some(midnight) = date.and_hms_opt(0, 0, 0) {
            return Ok(Utc.from_utc_datetime(&midnight));
        }
    }
    Err(format!("invalid datetime {text:?}"))
}

/// `deserialize_with` for `DateTime<Utc>`.
pub(crate) fn datetime<'de, D: Deserializer<'de>>(de: D) -> Result<DateTime<Utc>, D::Error> {
    let value = Value::deserialize(de)?;
    parse(&value).map_err(serde::de::Error::custom)
}

/// `deserialize_with` for `Option<DateTime<Utc>>`; `null` is `None`.
pub(crate) fn opt_datetime<'de, D: Deserializer<'de>>(
    de: D,
) -> Result<Option<DateTime<Utc>>, D::Error> {
    match Value::deserialize(de)? {
        Value::Null => Ok(None),
        value => parse(&value).map(Some).map_err(serde::de::Error::custom),
    }
}

#[cfg(test)]
mod tests {
    use super::parse;
    use serde_json::json;

    #[test]
    fn accepts_what_pydantic_accepts() {
        let expected = "2026-10-18T09:30:15Z";
        for input in [
            json!("2026-10-18T09:30:15Z"),
            json!("2026-10-18T09:30:15+00:00"),
            json!("2026-10-18T11:30:15+02:00"),
            json!("2026-10-18T09:30:15"),
            json!("2026-10-18 09:30:15"),
            json!(1_792_315_815),
        ] {
            let parsed = parse(&input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(
                parsed.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                expected,
                "{input}"
            );
        }
        assert_eq!(
            parse(&json!("2026-10-18"))
                .unwrap()
                .to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            "2026-10-18T00:00:00Z"
        );
        assert_eq!(
            parse(&json!("2026-10-18T09:30:15.123456"))
                .unwrap()
                .timestamp_subsec_micros(),
            123_456
        );
        assert!(parse(&json!("yesterday")).is_err());
        assert!(parse(&json!(true)).is_err());
    }
}
