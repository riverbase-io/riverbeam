# Wire-compatibility (dual-run)

Golden fixtures defining the canonical JSON wire shapes shared between the Rust
`riverbeam` stack and the Python `riverbeam` stack. The goal: a client can
switch which stack serves the API or the worker without any payload changes.

## Fixtures (`fixtures/`)

| Fixture | Type | Direction |
| --- | --- | --- |
| `invoke_request.json` | `InvokeRequest` | API → worker |
| `agent_result.json` | `AgentResult` | worker → API |
| `stream_chunk_done.json` | `StreamChunk` (done) | worker → SSE |
| `action_response.json` | `ActionResponseEnvelope` | API → worker (HITL resume) |

## Roundtrip test

`crates/beam_types/tests/wire_compat.rs` asserts each fixture deserializes into
its Rust type and re-serializes to a byte-equivalent JSON value. This locks
field names, casing, and the canonical "omit empty metadata" rule.

## Error codes

`beam_core` owns the Riverbase `BEM-*` catalogue (`declare_errors!`). Raise sites
return `RiverbaseError`; NATS replies serialize that object, and HTTP maps
`err.http_status`. Payload fixtures above are unchanged.

## Topologies (host-bound, deferred)

Running the cross-stack topologies requires both stacks plus shared Postgres
(schema `ref_beam`) and a broker:

- **Topology A:** Python API → Rust worker.
- **Topology B:** Rust API → Python worker.

Catalog continuity (session/message/HITL action written by one stack, read or
resumed by the other) is validated through the shared `ref_beam` schema in
`migrations/0001_beam_schema.sql`. A `docker-compose.dual-run.yml` wiring both
processes to shared infra is tracked in `docs/backlog.md`.
