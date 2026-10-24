//! `USAGE_BACKEND` selection (Python `usage/factory.py`).

use std::sync::Arc;

use beam_store::PgBeamStore;
use riverbase_core::RiverbaseResult;

use crate::pg::{PgReservationStore, PgUsageStore};
use crate::reservation::ReservationStore;
use crate::store::{InMemoryUsageStore, UsageStore};

/// Backend keys Rust accepts. Python's `redis` is not one (D4).
pub const USAGE_BACKENDS: &[&str] = &["inmemory", "in_memory", "memory", "sql"];

/// The usage store for `backend` (empty means `in_memory`, as in Python).
/// `sql` needs the beam store's database.
///
/// # Errors
/// `BEM-365` for an unknown key (the message lists the accepted ones), or for
/// `sql` without a database.
pub fn build_usage_store(
    backend: &str,
    pg: Option<&PgBeamStore>,
) -> RiverbaseResult<Arc<dyn UsageStore>> {
    match backend.trim() {
        "" | "inmemory" | "in_memory" | "memory" => Ok(Arc::new(InMemoryUsageStore::new())),
        "sql" => pg
            .map(|store| Arc::new(PgUsageStore::for_store(store)) as Arc<dyn UsageStore>)
            .ok_or_else(|| crate::BEM_365.with_data("USAGE_BACKEND=sql needs the Postgres store")),
        other => Err(crate::BEM_365.with_data(format!(
            "unknown USAGE_BACKEND '{other}'; available: {}",
            USAGE_BACKENDS.join(", ")
        ))),
    }
}

/// Shared reservations when there is a database (Python wires
/// `SqlReservationStore` whenever the store manager exists).
#[must_use]
pub fn build_reservation_store(pg: Option<&PgBeamStore>) -> Option<Arc<dyn ReservationStore>> {
    pg.map(|store| Arc::new(PgReservationStore::for_store(store)) as Arc<dyn ReservationStore>)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backends_match_python_keys() {
        assert!(build_usage_store("", None).is_ok());
        assert!(build_usage_store("in_memory", None).is_ok());
        let redis = build_usage_store("redis", None).err().unwrap();
        assert_eq!(redis.errcode.as_str(), "BEM-365");
        assert!(
            redis.to_string().contains("redis")
                || format!("{:?}", redis.errdata).contains("available")
        );
        assert!(build_usage_store("sql", None).is_err());
    }
}
