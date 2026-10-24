//! Agent specs, revisions, canary, enforcement, evals and feedback (ported
//! from fluvius-beam `agents/compiler.py`, `agents/enforcement.py`,
//! `orchestration/invocation/publication.py` and the revision steps of
//! `persistence.py`).
//!
//! Split by concern: spec checks, binding and pinning,
//! revision admin, evals, and the production profile.

mod admin;
mod bind;
mod evals;
mod production;
mod spec;

pub use spec::{
    assert_public_schemas_compatible, narrowed_max_steps, spec_from_registration, validate_spec,
};
