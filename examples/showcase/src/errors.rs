//! The error codes the new showcases share (`SHC-*`), in riverbase's error
//! shape. The loan desk keeps its own (`LND-*`).

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

/// The record store failed.
pub const SHC_001: ErrorSpec = spec(
    500,
    "Internal Server Error",
    "SHC-001",
    "The showcase record store failed.",
);
/// No such record, or not the caller's.
pub const SHC_002: ErrorSpec = spec(404, "Not Found", "SHC-002", "That record was not found.");
/// The record cannot change in its current status.
pub const SHC_003: ErrorSpec = spec(
    409,
    "Conflict",
    "SHC-003",
    "The record cannot change in its current status.",
);
/// The caller's role may not do this.
pub const SHC_004: ErrorSpec = spec(
    403,
    "Forbidden",
    "SHC-004",
    "The caller's role may not do this.",
);
/// The request or a tool's arguments are malformed.
pub const SHC_005: ErrorSpec = spec(400, "Bad Request", "SHC-005", "The request is malformed.");
