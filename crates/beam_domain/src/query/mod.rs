//! The beam queries (plans 007, 008, 013): one riverbase `query_resource!`
//! per resource, each over its table `<name>` in the beam schema (the pool's
//! `search_path`, see `SchemaName::pool_url`) through the table's
//! `pg_domain_entity!` in [`beam_store::entity`], served by the
//! `query_engine!` engine [`BeamQueryEngine`]. Migration 0005 gives the
//! tables riverbase's entity shape (`_etag` uuid, `_tenant`, `_txt`, owner
//! columns).
//!
//! The resources and entities are generated (`resources/`,
//! `beam_store/src/entity/`): edit `crates/beam_domain/query/spec.json` and
//! run `scripts/gen_query_resources.py`. Who sees which rows is [`policy`],
//! written by hand.

pub mod policy;
pub mod resources;

pub use resources::BeamQueryEngine;
