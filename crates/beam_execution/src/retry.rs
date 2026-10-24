//! Retry classification (port of fluvius-beam `execution/retry.py`).

/// Whether a failed execution may be retried.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryClass {
    Retryable,
    NotRetryable,
}

/// Public error codes that are never retried (`_NOT_RETRYABLE_CODES`).
pub const NOT_RETRYABLE_CODES: &[&str] = &[
    "BEM-400", "BEM-401", "BEM-402", "BEM-403", "BEM-410", "BEM-411", "BEM-412", "BEM-425",
    "BEM-426", "BEM-429",
];

/// `classify_error(code, waiting_for_input=...)`. `code` is a **public** code
/// (see `beam_core::public_errors`), as stored in `agent_execution.last_error_code`.
#[must_use]
pub fn classify_error(code: Option<&str>, waiting_for_input: bool) -> RetryClass {
    if waiting_for_input || code.is_some_and(|c| NOT_RETRYABLE_CODES.contains(&c)) {
        RetryClass::NotRetryable
    } else {
        RetryClass::Retryable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_like_python() {
        assert_eq!(
            classify_error(Some("BEM-429"), false),
            RetryClass::NotRetryable
        );
        assert_eq!(
            classify_error(Some("BEM-430"), false),
            RetryClass::Retryable
        );
        assert_eq!(classify_error(None, false), RetryClass::Retryable);
        assert_eq!(
            classify_error(Some("BEM-430"), true),
            RetryClass::NotRetryable
        );
    }
}
