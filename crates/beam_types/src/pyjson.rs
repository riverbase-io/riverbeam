//! Python-compatible JSON text, for values fluvius-beam stores as `json.dumps(...)` strings.
//!
//! [`dumps`] matches `json.dumps(value)` with default arguments: `", "` and
//! `": "` separators, keys in insertion order, and `ensure_ascii=True` (non-ASCII
//! escaped as `\uXXXX`, with surrogate pairs above U+FFFF).

use std::fmt::Write as _;

use chrono::{DateTime, Timelike, Utc};
use serde_json::Value;

/// `json.dumps(value)` with Python's default formatting.
#[must_use]
pub fn dumps(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value, true);
    out
}

/// `json.dumps(value, ensure_ascii=False)`: the same, with non-ASCII text kept
/// as is (`LangChain`'s tool-message content).
#[must_use]
pub fn dumps_unicode(value: &Value) -> String {
    let mut out = String::new();
    write_value(&mut out, value, false);
    out
}

/// `json.dumps(value, sort_keys=True, separators=(",", ":"))`: compact, keys
/// sorted, ASCII-escaped (used for content hashes such as policy revisions).
#[must_use]
pub fn dumps_sorted_compact(value: &Value) -> String {
    let mut out = String::new();
    write_sorted(&mut out, value, true);
    out
}

/// `json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`:
/// the same with non-ASCII text kept (Python `canonical_spec_hash`).
#[must_use]
pub fn dumps_sorted_compact_unicode(value: &Value) -> String {
    let mut out = String::new();
    write_sorted(&mut out, value, false);
    out
}

fn write_sorted(out: &mut String, value: &Value, ascii: bool) {
    match value {
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_sorted(out, item, ascii);
            }
            out.push(']');
        }
        Value::Object(map) => {
            // UTF-8 byte order is code-point order, as Python sorts str keys.
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push('{');
            for (i, key) in keys.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_str(out, key, ascii);
                out.push(':');
                write_sorted(out, &map[key], ascii);
            }
            out.push('}');
        }
        other => write_value(out, other, ascii),
    }
}

/// Python's `repr()` (what `str()` gives) of a JSON-like value: `None`,
/// `True`/`False`, single-quoted strings, `{'k': v}` and `[a, b]`.
#[must_use]
pub fn py_repr(value: &Value) -> String {
    let mut out = String::new();
    write_repr(&mut out, value);
    out
}

fn write_repr(out: &mut String, value: &Value) {
    match value {
        Value::Null => out.push_str("None"),
        Value::Bool(b) => out.push_str(if *b { "True" } else { "False" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => repr_str(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_repr(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                repr_str(out, key);
                out.push_str(": ");
                write_repr(out, item);
            }
            out.push('}');
        }
    }
}

/// Python's `str.__repr__`: single quotes unless the text has a single quote
/// and no double quote; printable non-ASCII kept as is.
fn repr_str(out: &mut String, s: &str) {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    out.push(quote);
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if (c as u32) < 0x20 || c as u32 == 0x7f => {
                let _ = write!(out, "\\x{:02x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push(quote);
}

/// A datetime as Pydantic's `model_dump(mode="json")` writes it: RFC 3339 with
/// `Z`, microseconds when non-zero, no fraction otherwise.
#[must_use]
pub fn pydantic_datetime(dt: DateTime<Utc>) -> String {
    let micros = dt.nanosecond() / 1_000 % 1_000_000;
    if micros == 0 {
        dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    } else {
        format!("{}.{micros:06}Z", dt.format("%Y-%m-%dT%H:%M:%S"))
    }
}

fn write_value(out: &mut String, value: &Value, ascii: bool) {
    match value {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => out.push_str(&n.to_string()),
        Value::String(s) => write_str(out, s, ascii),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_value(out, item, ascii);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (key, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                write_str(out, key, ascii);
                out.push_str(": ");
                write_value(out, item, ascii);
            }
            out.push('}');
        }
    }
}

fn write_str(out: &mut String, s: &str, ascii: bool) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            c if c.is_ascii() && !c.is_ascii_control() => out.push(c),
            // `ensure_ascii=False` escapes only ASCII controls, quotes and backslashes.
            c if !ascii && !c.is_ascii() => out.push(c),
            c => {
                let mut units = [0u16; 2];
                for unit in c.encode_utf16(&mut units) {
                    let _ = write!(out, "\\u{unit:04x}");
                }
            }
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;

    /// Expected strings are Python 3.13 `json.dumps` output for the same values.
    #[test]
    fn matches_python_json_dumps() {
        assert_eq!(
            dumps(&json!({"a": 1, "b": [true, null, "x"], "c": {"d": 1.5}})),
            r#"{"a": 1, "b": [true, null, "x"], "c": {"d": 1.5}}"#
        );
        assert_eq!(
            dumps(&json!("ti\u{1ebf}ng Vi\u{1ec7}t")),
            r#""ti\u1ebfng Vi\u1ec7t""#
        );
        assert_eq!(dumps(&json!("\u{1f600}")), r#""\ud83d\ude00""#);
        assert_eq!(dumps(&json!("a\"b\\c\n\u{1}")), r#""a\"b\\c\n\u0001""#);
        assert_eq!(dumps(&json!([])), "[]");
        assert_eq!(dumps(&json!({})), "{}");
        assert_eq!(
            dumps_unicode(&json!({"t": "ti\u{1ebf}ng"})),
            "{\"t\": \"ti\u{1ebf}ng\"}"
        );
    }

    /// Expected strings are Python 3.13 output for the same values.
    #[test]
    fn repr_and_sorted_dumps_match_python() {
        let v = json!([{"id": "c1", "args": {"a": 1, "ok": true, "n": null}, "name": "it's"}]);
        assert_eq!(
            py_repr(&v),
            r#"[{'id': 'c1', 'args': {'a': 1, 'ok': True, 'n': None}, 'name': "it's"}]"#
        );
        assert_eq!(py_repr(&json!("a\nb\u{1}")), r"'a\nb\x01'");
        assert_eq!(
            dumps_sorted_compact(&json!({"b": [1, "x"], "a": null, "\u{e9}": true})),
            r#"{"a":null,"b":[1,"x"],"\u00e9":true}"#
        );
    }

    #[test]
    fn datetimes_match_pydantic() {
        let whole = Utc.with_ymd_and_hms(2026, 10, 18, 9, 30, 15).unwrap();
        assert_eq!(pydantic_datetime(whole), "2026-10-18T09:30:15Z");
        let frac = whole + chrono::Duration::nanoseconds(123_456_789);
        assert_eq!(pydantic_datetime(frac), "2026-10-18T09:30:15.123456Z");
    }
}
