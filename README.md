# riverbeam

AI application server. It runs an invoke pipeline for agents: session and run tracking, memory, retrieval, model routing, quota, audit, security checks, and streamed output. The domain and worker machinery comes from [riverbase](https://github.com/riverbase-io/riverbase). Agent execution uses [rig-core](https://rig.rs). Ingest statistics use Polars.

This repository is the library workspace. It does not include the example application.

Check out `riverbase` and `river-reed` as siblings of this directory. The workspace manifest depends on them at `../riverbase/crates/riverbase_core` and `../river-reed`.

## Pipeline

`RiverBeam` in `beam_core` runs one invocation in order:

1. Bootstrap the session and run, and check audit and quota.
2. Save the input turn, or resume a human-in-the-loop action.
3. Build context from memory and retrieved knowledge, after the security check.
4. Dispatch the agent and stream chunks.
5. Persist the result and close the run.

```text
API process                              Worker process
  beam_domain                              NatsWorkerServer
    invoke  ── NATS request/reply ──▶        beam_worker ──▶ beam_core
    stream  ── NATS publish ─────────▶                         │
    frames  ◀── NATS stream ────────── beam_stream             └─▶ Postgres (ref_beam)
```

## Crates

| Crate | Role |
| --- | --- |
| `beam_types` | Wire types: `InvokeRequest`, `AgentResult`, `StreamChunk`, actions |
| `beam_agents` | `Agent` trait, registry, rig runner, sandbox tools, skills, OpenAI-compatible HTTP |
| `beam_sandbox` | Path-jailed workspace and working-tree git. Protected refs are never pushed |
| `beam_routing` | `ModelRouter` with modality, capability, cost, and fallback strategies. YAML model pools |
| `beam_memory` | Hierarchical memory scopes and a context-trim strategy |
| `beam_knowledge` | Embedder, vector store, retriever, chunker, and ingest. Polars stats are optional |
| `beam_store` | Persistence façade. `PgBeamStore` uses schema `ref_beam` |
| `beam_usage` | Usage plans, quota guard, and consumption tracking |
| `beam_audit` | Structured audit events and pluggable sinks |
| `beam_security` | Prompt, output, and tool guards evaluated with `river_reed` |
| `beam_audio` | Realtime audio session, speech-to-text, and text-to-speech |
| `beam_stream` | Stream transport and chunk-merge buffer: memory, broadcast, or NATS |
| `beam_core` | `RiverBeam` invoke pipeline and the `BEM-*` error catalogue |
| `beam_worker` | Worker RPC: invoke, knowledge ingest, and memory. `NatsWorkerServer` consumes NATS |
| `beam_domain` | Command and query surface: `invoke`, `stream`, `resume`, and the catalog. Casbin authorization |

## Persistence

`migrations/0001_beam_schema.sql` creates schema `ref_beam`. It holds the catalog (`model`, `agent`), the execution hierarchy (`agent_session`, `agent_interaction`, `agent_run`, `agent_message`, `agent_action`), and the pgvector tables for knowledge and memory. The embedding width is 1536.

`PgBeamStore::connect` creates the non-vector execution tables so a stock Postgres can boot. Apply the full migration when knowledge or memory vectors are required. That needs the `vector` extension.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BEAM_DATABASE_URL` | — | Postgres URL for the store and knowledge tables. `DATABASE_URL` is also read |
| `NATS_URL` | `nats://127.0.0.1:4222` | Bus for worker RPC and stream frames |
| `BEAM_BIND_ADDR` | `0.0.0.0:8080` | HTTP listen address when an application mounts the domain |

Security rules ship with `beam_security` and evaluate through `river_reed`. Enable that crate's `grl-rule-engine` feature, which this workspace already selects.

## Build

Requires Rust 1.78 or newer. Postgres is required for store and knowledge tests. NATS is required for worker tests.

```bash
cargo build --workspace
cargo test --workspace
```

Polars ingest stats and the pgvector store are feature-gated:

```bash
cargo test -p beam_knowledge --features polars-ingest
BEAM_DATABASE_URL=postgres://USER@127.0.0.1:5432/beam \
  cargo test -p beam_knowledge --features postgres
```

## Errors

`beam_core` owns the `BEM-*` catalogue. Raise sites return `RiverbaseError`. NATS replies serialize that error, and an HTTP adapter maps it to a status code. `BEM-060` is 403. `BEM-045` is 501 for a worker task that is not implemented yet.

## License

RFX JSC Proprietary License. See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT).
