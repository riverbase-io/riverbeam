//! riverbeam showcases: real applications on one server.
//!
//! - [`loan_desk`]: a mortgage lender's desk (borrowers, loan officers, an
//!   admin).
//! - [`support`]: a store's customer support (customers and support agents).
//! - [`claims`]: auto insurance claims (policyholders and adjusters).
//! - [`incident`]: an on-call incident desk (SREs and the incident commander).
//! - [`invoice`]: accounts payable (AP clerks and the controller).
//!
//! [`registry`] lists them; `showcase-server` binds every showcase's agents on
//! one beam and the UI switches between them. [`host`] is the wiring they
//! share.

pub mod aliases;
pub mod claims;
pub mod errors;
pub mod host;
pub mod incident;
pub mod invoice;
pub mod kit;
pub mod loan_desk;
pub mod mcp_stub;
pub mod records;
pub mod registry;
pub mod support;
