//! The loan desk's error codes (`LND-*`), in riverbase's error shape.

use riverbase_core::base::ErrorSpec;

const BASE: &str = "https://riverbase.io/~/rs/error/";

const fn spec(
    http_status: u16,
    http_title: &'static str,
    errcode: &'static str,
    errmesg: &'static str,
) -> ErrorSpec {
    ErrorSpec {
        error_type_base: BASE,
        http_status,
        http_title,
        errcode,
        errmesg,
        errhint: None,
    }
}

/// The application store failed.
pub const LND_001: ErrorSpec = spec(
    500,
    "Internal Server Error",
    "LND-001",
    "The loan application store failed.",
);
/// No such application, or not the caller's.
pub const LND_002: ErrorSpec = spec(
    404,
    "Not Found",
    "LND-002",
    "That loan application was not found.",
);
/// The application cannot change in its current status.
pub const LND_003: ErrorSpec = spec(
    409,
    "Conflict",
    "LND-003",
    "The loan application cannot change in its current status.",
);
/// The application is missing what it needs to be submitted.
pub const LND_004: ErrorSpec = spec(
    422,
    "Unprocessable Content",
    "LND-004",
    "The loan application is not complete.",
);
/// The caller's role may not do this.
pub const LND_005: ErrorSpec = spec(
    403,
    "Forbidden",
    "LND-005",
    "This needs a loan officer or an admin.",
);
/// No such loan program.
pub const LND_006: ErrorSpec = spec(
    404,
    "Not Found",
    "LND-006",
    "That loan program does not exist.",
);
/// The request is malformed.
pub const LND_007: ErrorSpec = spec(400, "Bad Request", "LND-007", "The request is malformed.");
