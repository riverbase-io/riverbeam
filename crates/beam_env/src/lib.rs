//! One parser for riverbeam's environment keys.
//!
//! Each crate keeps its own config struct, whose `Default` reads nothing and
//! whose `from_env()` reads its keys through these functions, so every key
//! parses the same way:
//!
//! - unset or blank means the default;
//! - a flag is true for `1`, `true`, `yes`, `on` and false for `0`, `false`,
//!   `no`, `off` (trimmed, any case); anything else means the default, with a
//!   warning (plan 021 D2);
//! - a number that does not parse means the default.

/// `key`'s value, trimmed; `None` when unset or blank.
#[must_use]
pub fn string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_owned())
        .filter(|v| !v.is_empty())
}

/// A flag's raw value: `Some(true)` or `Some(false)` for the words above,
/// `None` for anything else.
#[must_use]
pub fn parse_flag(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Flag `key`, or `default` when unset, blank or not a flag word.
#[must_use]
pub fn flag(key: &str, default: bool) -> bool {
    let Some(raw) = string(key) else {
        return default;
    };
    parse_flag(&raw).unwrap_or_else(|| {
        tracing::warn!(key, value = %raw, default, "not a flag value; using the default");
        default
    })
}

/// Number `key`, or `None` when unset, blank or unparsable.
#[must_use]
pub fn number<T: std::str::FromStr>(key: &str) -> Option<T> {
    string(key).and_then(|v| v.parse().ok())
}

/// List `key`: a JSON list (`["stdout","file"]`) or a comma-separated one.
#[must_use]
pub fn list(key: &str) -> Option<Vec<String>> {
    string(key).map(|raw| parse_list(&raw))
}

/// A list's raw value (see [`list`]).
#[must_use]
pub fn parse_list(raw: &str) -> Vec<String> {
    if let Ok(list) = serde_json::from_str::<Vec<String>>(raw) {
        return list;
    }
    raw.split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Seconds `key`: a positive, finite number; `None` otherwise.
#[must_use]
pub fn secs(key: &str) -> Option<f64> {
    number::<f64>(key).filter(|s| s.is_finite() && *s > 0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_words() {
        for on in ["1", "true", "YES", " on "] {
            assert_eq!(parse_flag(on), Some(true), "{on}");
        }
        for off in ["0", "False", "no", "OFF "] {
            assert_eq!(parse_flag(off), Some(false), "{off}");
        }
        for neither in ["", "garbage", "2"] {
            assert_eq!(parse_flag(neither), None, "{neither}");
        }
    }

    #[test]
    fn lists_accept_json_and_commas() {
        assert_eq!(parse_list(r#"["stdout","sql"]"#), ["stdout", "sql"]);
        assert_eq!(parse_list("stdout, file,"), ["stdout", "file"]);
    }
}
