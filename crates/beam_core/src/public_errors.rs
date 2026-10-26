//! Public error contract shared with the Python fluvius-beam stack (plan 005, D3).
//!
//! Rust keeps one `BEM-*` code per raise site (riverbase convention). Clients
//! see Python's public codes instead: each site maps to one of Python's
//! `kernel/errors.py` class codes, or to the Fluvius platform code Python emits
//! for the same situation (`FLR-001` for unhandled internal failures, `AUT-003`
//! for authorization denials, `WEB-001` for payload validation). The Rust site code moves to `errdata.site_code`.
//!
//! Apply [`to_public`] only where an error leaves riverbeam for a client (HTTP
//! bodies, SSE error frames). Internal hops such as the NATS worker reply keep
//! the site code.

use std::fmt::Write as _;

use riverbase_core::{ProblemDetails, RiverbaseError, RiverbaseErrorCode};
use serde_json::{Map, Value};

/// A public code with the HTTP status and title Python uses for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublicCode {
    pub code: &'static str,
    pub status: u16,
    pub title: &'static str,
}

const BAD_REQUEST: &str = "Bad Request";
const FORBIDDEN: &str = "Forbidden";
const UNPROCESSABLE: &str = "Unprocessable Entity";

/// Python's public codes (`fluvius_beam.kernel.errors`) plus the Fluvius codes
/// Python surfaces for errors that are not `BEM-*` classes.
pub const PUBLIC_CODES: &[PublicCode] = &[
    PublicCode {
        code: "BEM-400",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-401",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-402",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-403",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "BEM-404",
        status: 404,
        title: "Not Found",
    },
    PublicCode {
        code: "BEM-410",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-411",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "BEM-412",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "BEM-422",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-423",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-424",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-425",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-426",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-427",
        status: 400,
        title: BAD_REQUEST,
    },
    PublicCode {
        code: "BEM-428",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-429",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "BEM-430",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-432",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-433",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "BEM-434",
        status: 422,
        title: UNPROCESSABLE,
    },
    PublicCode {
        code: "BEM-435",
        status: 503,
        title: "Service Unavailable",
    },
    PublicCode {
        code: "FLR-001",
        status: 500,
        title: "Internal Server Error",
    },
    PublicCode {
        code: "AUT-003",
        status: 403,
        title: FORBIDDEN,
    },
    PublicCode {
        code: "WEB-001",
        status: 422,
        title: UNPROCESSABLE,
    },
];

/// The public code for a Rust site code, or `None` when the code is not a
/// riverbeam `BEM-*` site (riverbase platform codes pass through unchanged).
#[must_use]
// Arms are grouped by meaning, with a comment each, rather than merged by result.
#[allow(clippy::match_same_arms)]
pub fn public_code(site: &str) -> Option<&'static str> {
    let serial: u16 = site.strip_prefix("BEM-")?.parse().ok()?;
    let code = match serial {
        // Request decoding and payload shape.
        1 | 2 => "FLR-001",
        3 => "WEB-001",
        4..=6 => "BEM-400",
        // Unknown agents, commands, resources, catalog rows.
        20..=26 => "BEM-404",
        // Resume referenced an action that is not there.
        27 | 28 => "BEM-402",
        40 => "BEM-401",
        41 => "BEM-422",
        42..=47 => "FLR-001",
        60 => "BEM-403",
        // Security block and command authorization deny.
        61 | 67 => "AUT-003",
        62..=66 => "BEM-402",
        68 | 70 => "BEM-400",
        // Python `TenantAccessError`.
        69 => "BEM-411",
        // Python `SessionOwnershipError`.
        71 => "BEM-412",
        // Model calls and the agent loop fail inside the runtime.
        80..=85 | 87..=90 => "BEM-430",
        86 => "BEM-423",
        91..=94 | 98..=106 => "FLR-001",
        // The API could not hand the task to the worker queue.
        95..=97 => "BEM-435",
        107 | 108 => "BEM-400",
        // Store failures, entity writes included (BEM-145, BEM-146),
        // and a beam's build errors (BEM-147, BEM-149).
        109..=149 => "FLR-001",
        200..=206 | 208..=227 | 231..=234 => "FLR-001",
        207 | 230 | 237 | 238 | 240 | 245 => "BEM-400",
        235 | 236 | 241..=244 => "FLR-001",
        239 => "BEM-404",
        300..=306 | 308 | 309 => "BEM-400",
        307 | 310..=318 | 320..=327 | 329..=332 => "FLR-001",
        328 => "AUT-003",
        350 | 351 | 354 => "BEM-400",
        352 | 353 => "BEM-424",
        355..=360 => "FLR-001",
        // Usage: quota denials are Python's QuotaExceededError.
        361 | 364 | 365 => "FLR-001",
        362 | 363 => "BEM-403",
        // Execution control plane.
        370 => "BEM-425",
        371 => "BEM-426",
        372 | 375 | 376 | 378 => "BEM-429",
        373 | 379 => "BEM-427",
        374 => "BEM-428",
        377 => "BEM-404",
        380..=383 => "FLR-001",
        384 => "BEM-424",
        // Runtime contract.
        390 => "BEM-423",
        391 => "BEM-424",
        392..=395 | 398 => "BEM-430",
        396 => "FLR-001",
        397 => "BEM-429",
        399 => "BEM-402",
        // Model gateway: provider failures surface as
        // Python's RuntimeExecutionError; configuration errors are internal.
        460 | 463..=465 => "BEM-430",
        461 | 462 => "FLR-001",
        466 => "BEM-422",
        // Tool runtime and MCP.
        470 | 472 => "BEM-430",
        471 | 474 => "FLR-001",
        // Context pipeline.
        // 484: a model call over its model's window.
        480 | 484 => "BEM-432",
        481 => "BEM-401",
        482 => "BEM-427",
        483 => "BEM-428",
        485..=487 => "FLR-001",
        // Stream and operations.
        488 => "BEM-404",
        // Python `PrivacyHoldError`.
        489 => "BEM-433",
        // Revisions and evals: Python's
        // ResumeValidationError, ExecutionTransitionError,
        // CheckpointUnreadableError, BeamError. 495 is retired.
        490 | 491 => "BEM-402",
        492 => "BEM-425",
        493 => "BEM-430",
        494 => "BEM-400",
        496 => "BEM-404",
        // Domain and HTTP app.
        497 => "FLR-001",
        498 => "BEM-410",
        499 | 500 | 502 => "BEM-400",
        501 => "BEM-404",
        // Audio backends (sidecars, see plan 005 D8).
        503..=506 => "FLR-001",
        // Work queue and macro-mode domain.
        507 | 509 | 510 => "FLR-001",
        // No worker ran it: Python's queue-unavailable code.
        508 => "BEM-435",
        _ => return None,
    };
    Some(code)
}

fn lookup(code: &str) -> Option<&'static PublicCode> {
    PUBLIC_CODES.iter().find(|p| p.code == code)
}

/// Rewrite a riverbeam error into the public contract.
///
/// The code, status and title become Python's. The original site code is kept
/// in `errdata.site_code`; non-object `errdata` moves to `errdata.detail`.
/// Errors that are not riverbeam `BEM-*` sites, and errors already rewritten
/// (they carry `errdata.site_code`), are returned unchanged.
#[must_use]
pub fn to_public(err: RiverbaseError) -> RiverbaseError {
    if err.errdata.get("site_code").is_some() {
        return err;
    }
    let site = err.errcode.as_str().to_owned();
    let Some(public) = public_code(&site).and_then(lookup) else {
        return err;
    };
    let mut data = match err.errdata {
        Value::Object(map) => map,
        Value::Null => Map::new(),
        other => {
            let mut map = Map::new();
            map.insert("detail".into(), other);
            map
        }
    };
    data.insert("site_code".into(), Value::from(site));
    let type_base = err.errcode.type_base();
    let mut out = RiverbaseError::new(
        public.status,
        public.title,
        RiverbaseErrorCode::from_static_with_base(public.code, type_base),
        err.errmesg,
        Value::Object(data),
    );
    out.errhint = err.errhint;
    out
}

/// RFC 9457 problem body for an HTTP error response, in the public contract.
#[must_use]
pub fn problem_details(err: RiverbaseError, instance: Option<String>) -> ProblemDetails {
    ProblemDetails::from_riverbase_error(&to_public(err), instance)
}

/// The `error` string of an SSE error frame, formatted like Python's
/// `str(FluviusException)`: `[status] CODE: message`, then optional
/// ` [DATA: ...]` and ` [HINT: ...]`.
#[must_use]
pub fn stream_error_text(err: RiverbaseError) -> String {
    let err = to_public(err);
    let mut text = format!(
        "[{}] {}: {}",
        err.http_status,
        err.errcode.as_str(),
        err.errmesg
    );
    if !err.errdata.is_null() {
        let _ = write!(text, " [DATA: {}]", err.errdata);
    }
    if let Some(hint) = &err.errhint {
        let _ = write!(text, " [HINT: {hint}]");
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Every `BEM-*` code declared in the catalogue, read from its source.
    fn catalogue_codes() -> Vec<(String, u16)> {
        // The catalog crate's source: test code only.
        let source = include_str!("../../beam_errors/src/lib.rs");
        let mut out = Vec::new();
        let mut status = None;
        for line in source.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("status:") {
                status = rest.trim().trim_end_matches(',').parse().ok();
            }
            if let Some(rest) = line.strip_prefix("code: \"") {
                let code = rest.split('"').next().unwrap().to_owned();
                out.push((code, status.take().expect("status before code")));
            }
        }
        out
    }

    #[test]
    fn every_site_maps_to_a_public_code() {
        let codes = catalogue_codes();
        assert!(
            codes.len() > 100,
            "catalogue parse found {} codes",
            codes.len()
        );
        for (site, _) in &codes {
            let public = public_code(site).unwrap_or_else(|| panic!("{site} has no public code"));
            assert!(
                lookup(public).is_some(),
                "{site} maps to unknown public code {public}"
            );
        }
    }

    /// Sites whose Rust status is a 4xx although they are internal failures
    /// (serializing our own payloads, publishing to our own transport, routing
    /// to a worker task that does not exist). Python reports these as `FLR-001` (500).
    const INTERNAL_SITES_WITH_4XX_STATUS: &[&str] =
        &["BEM-001", "BEM-002", "BEM-091", "BEM-105", "BEM-106"];

    #[test]
    fn client_errors_stay_client_errors() {
        // A 4xx site never becomes a 5xx public code, and a 5xx site never
        // becomes a 4xx one, apart from the named internal sites above.
        for (site, status) in catalogue_codes() {
            if INTERNAL_SITES_WITH_4XX_STATUS.contains(&site.as_str()) {
                assert_eq!(public_code(&site), Some("FLR-001"));
                continue;
            }
            let public = lookup(public_code(&site).unwrap()).unwrap();
            assert_eq!(
                status >= 500,
                public.status >= 500,
                "{site} ({status}) -> {} ({})",
                public.code,
                public.status
            );
        }
    }

    #[test]
    fn to_public_rewrites_code_and_keeps_site() {
        let err = crate::BEM_020.with_data(json!({"agent": "x"}));
        let public = to_public(err);
        assert_eq!(public.errcode.as_str(), "BEM-404");
        assert_eq!(public.http_status, 404);
        assert_eq!(public.errdata["site_code"], "BEM-020");
        assert_eq!(public.errdata["agent"], "x");
    }

    /// The domain engines rewrite errors, and the HTTP layer rewrites them
    /// again: a public code must never be read as a site code.
    #[test]
    fn to_public_is_idempotent() {
        for (site, status) in catalogue_codes() {
            let code: &'static str = Box::leak(site.clone().into_boxed_str());
            let err = RiverbaseError::new(
                status,
                "Site",
                RiverbaseErrorCode::from_static_with_base(
                    code,
                    crate::BEM_001.raise().errcode.type_base(),
                ),
                "site error",
                Value::Null,
            );
            let once = to_public(err);
            let twice = to_public(once.clone());
            assert_eq!(twice.errcode.as_str(), once.errcode.as_str(), "{site}");
            assert_eq!(twice.errdata["site_code"], site.as_str(), "{site}");
        }
    }

    #[test]
    fn to_public_wraps_scalar_errdata() {
        let public = to_public(crate::BEM_096.with_data("connection refused"));
        assert_eq!(public.errcode.as_str(), "BEM-435");
        assert_eq!(public.http_status, 503);
        assert_eq!(public.errdata["detail"], "connection refused");
    }

    #[test]
    fn platform_codes_pass_through() {
        let err = RiverbaseError::new(
            404,
            "Not Found",
            RiverbaseErrorCode::new("DAT-001"),
            "missing",
            Value::Null,
        );
        assert_eq!(to_public(err).errcode.as_str(), "DAT-001");
    }

    #[test]
    fn http_body_matches_python_problem_details() {
        let python: Value = serde_json::from_str(include_str!(
            "../../../tests/wire_compat/fixtures/error_http_body.json"
        ))
        .unwrap();
        let rust = serde_json::to_value(problem_details(
            crate::BEM_020.raise(),
            python["instance"].as_str().map(str::to_owned),
        ))
        .unwrap();
        for key in ["title", "status", "errcode", "instance"] {
            assert_eq!(rust[key], python[key], "problem field {key}");
        }
        // `type` differs by design (riverbase vs fluvius URI base) but ends in the code.
        assert!(rust["type"]
            .as_str()
            .unwrap()
            .ends_with(python["errcode"].as_str().unwrap()));
    }

    #[test]
    fn stream_error_text_matches_python_prefix() {
        let python: Value = serde_json::from_str(include_str!(
            "../../../tests/wire_compat/fixtures/error_stream_frame.json"
        ))
        .unwrap();
        let python_text = python["error"].as_str().unwrap();
        let rust_text = stream_error_text(crate::BEM_020.raise());
        // Rust appends ` [DATA: {"site_code": ...}]`; everything before it matches.
        assert!(
            rust_text.starts_with(python_text),
            "{rust_text} vs {python_text}"
        );
        assert!(
            rust_text.contains("\"site_code\":\"BEM-020\""),
            "{rust_text}"
        );
    }

    /// The hand-copied status and title table matches Python's error classes.
    #[test]
    fn public_codes_match_python_table() {
        let python: Value = serde_json::from_str(include_str!(
            "../../../tests/wire_compat/fixtures/error_codes.json"
        ))
        .unwrap();
        let table = python.as_object().unwrap();
        assert_eq!(table.len(), PUBLIC_CODES.len(), "code sets differ");
        for code in PUBLIC_CODES {
            let row = &table[code.code];
            assert_eq!(row["status"], code.status, "{} status", code.code);
            assert_eq!(row["title"], code.title, "{} title", code.code);
        }
    }
}
