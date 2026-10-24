# Wire compatibility (plan 005, Phase 01)

Golden fixtures for the JSON wire shapes shared by the Python fluvius-beam stack and the Rust riverbeam stack. A client must be able to switch the stack that serves it without any payload change.

## Fixtures (`fixtures/`)

The fixtures are **generated** from Python by `scripts/gen_wire_fixtures.py`. Never edit them by hand. `fixtures/BASELINE` records the fluvius-beam commit they came from.

Each wire type has a full fixture (`<name>.json`: every optional field set, dumped with `exclude_none=True`, the way Python publishes stream frames). Most also have a minimal one (`<name>_minimal.json`: required fields only, dumped with explicit `null`s, the way Python returns RPC results).

Covered types: `ContentPart` (all 8 part types), `InvokeAuthContext`, `RequestContext`, `InvokeRequest`, `ChatRequest`, `AgentInput`, `AgentResult`, `AgentMessage`, `StreamChunk` (`chunk`, `done`, `error`), `ActionResponseEnvelope`, `ActionRequestEnvelope`, `AgentAction`, `AgentStep`, `ToolCallRecord`, `TokenUsage`, `AgentMetadata`, `ModelConfig`, `Artifact`. In addition, `error_http_body.json` and `error_stream_frame.json` capture Python's client-facing error shapes.

Regenerate after rebasing the plan baseline:

```bash
just wire-fixtures /path/to/venv/bin/python
```

The interpreter needs fluvius-beam's dependencies (for example the `.venv` of a working fluvius-beam checkout). The recipe passes `--src ../fluvius-beam/src --src ../../fluvius-py/src`; the script refuses to run if `fluvius_beam` loads from anywhere else, since an installed copy in the venv could otherwise win.

## Tests

- `crates/beam_types/tests/wire_compat.rs`: each fixture deserializes into its Rust type and re-serializes to the same JSON after normalization. `{}` and `[]` are dropped on both sides, because Python emits some paths with `exclude_none` and some without, and each such field has the same default on both stacks. `null` is dropped from Python's side, but from Rust's side only where Python's payload had `null` at the same path. Elsewhere Rust must omit the field, because Pydantic rejects `null` for `dict` and `list` fields. Unknown fields must be ignored, as in Pydantic, and datetimes are read as leniently as Pydantic reads them (naive values as UTC). A test fails if a fixture file has no roundtrip test.
- `crates/beam_core/src/public_errors.rs`: the error fixtures check the public error contract (below), including Python's code → status and title table (`error_codes.json`).

## Stream frame types

Python's `StreamChunk.type` is only `chunk | done | error`. Rust used to add `placeholder`, `thinking` and `tool`, which Python clients reject. They are now `chunk` frames:

| Old Rust type | Now |
|---------------|-----|
| `placeholder` | `chunk` with `data.phase = "thinking"` and `data.metadata.placeholder = true` |
| `thinking` | `chunk` with `data.phase = "thinking"` |
| `tool` | `chunk` with `data.type = "step"`, plus `data.tool_name` and `data.tool_args` |

## Error codes (D3)

Rust keeps one `BEM-*` code per raise site (the `beam_errors` crate). Clients see Python's public codes instead: `beam_core::public_errors::to_public` maps every site to one of Python's 22 `BEM-*` class codes, or to the Fluvius code Python emits for the same case (`FLR-001` for internal failures, `AUT-003` for authorization denials, `WEB-001` for payload validation). It also sets Python's HTTP status and title. The site code moves to `errdata.site_code`.

The mapping applies only where an error leaves riverbeam for a client: the HTTP problem body (`public_errors::problem_details`), SSE error frames (`public_errors::stream_error_text`, formatted like Python's `[status] CODE: message`), and the domain's command and query engines (`beam_domain`, Phase 12), whose errors riverbase renders. `to_public` is idempotent: an error that already carries `errdata.site_code` passes through. Queued work keeps site codes in `agent_task.error` (plan 006). Clients must branch on `errcode` only.

## Identifiers

`beam_store::agent_id_for(name)` uses the Fluvius `UUID5_NAMESPACE`, so it equals Python's `UUID_GENF(agent_name)`, and both stacks key the same `agent` row.

## HTTP surface (Phase 12)

`http/python_openapi_paths.json` is Python's route set (paths and methods), dumped by `scripts/dump_python_openapi.py`. `examples/showcase/tests/openapi_paths.rs` checks the Rust app against it, and `just openapi-diff URL=…` checks a running server. Routes not served yet are listed with their reason in `http/openapi_pending.json`, which both read.

Durable stream replay is `GET /api/beam/sse/{session_id}/{request_id}` on both stacks, with `Last-Event-ID`; the framing is checked by `crates/beam_stream/tests/golden.rs` (Phase 10). Python's RTC bridge path, `/api/rtc/default~ssestream/{channel}`, is pending: riverbase's `RtcBridge` has no channel-authorization hook.
