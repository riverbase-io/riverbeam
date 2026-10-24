# riverbeam

AI application server. It runs an invoke pipeline for agents: session and run tracking, context building from memory and retrieved knowledge, model routing, quota, audit, security checks, and streamed output. The domain, command and worker machinery comes from [riverbase](https://github.com/riverbase-io/riverbase). Agents run on [rig-core](https://rig.rs).

This repository holds the library workspace (`crates/`) and one example server, `examples/showcase`.

Check out `riverbase` and `river-reed` as siblings of this directory. The workspace manifest depends on them at `../riverbase/crates/…` and `../river-reed`. Cargo needs `river-reed` present even in a build without the security rules.

## Pipeline

`RiverBeam` in `beam_core` runs one invocation in order:

1. Bootstrap the session and run, and check audit and quota.
2. Save the input turn, or resume a human-in-the-loop action.
3. Run the security checks, then build context from memory and retrieved knowledge.
4. Dispatch the agent and stream its frames.
5. Persist the result and close the run.

```text
API process (riverbase_http)                         Worker process
  commands ── one transaction: beam rows + agent_task    QueueWorker
              + riverbase command log + outbox ─hint─▶     │ claims rows (SKIP LOCKED + lease)
  queries  ── Postgres (BEAM_SQL_SCHEMA) ◀───────────────  │ beam_worker ──▶ beam_core
  SSE replay ◀── durable stream (outbox)                   └─▶ frames on riverbase's bus
```

Commands are riverbase typed handlers: each runs in one transaction, never waits for a worker, and queues agent runs as `agent_task` rows with a hint on riverbase's bus. A worker claims and runs them. Queries are riverbase query resources, filtered to the caller.

## Crates

| Crate | Role |
| --- | --- |
| `beam_types` | Wire types: `InvokeRequest`, `AgentResult`, `StreamChunk`, actions |
| `beam_errors` | The `BEM-*` error catalogue |
| `beam_env` | One parser for riverbeam's environment keys |
| `beam_core` | `RiverBeam` invoke pipeline (`RiverBeam::builder`) and readiness |
| `beam_domain` | riverbase typed commands that queue work, and 26 query resources; Casbin policy |
| `beam_worker` | Task dispatch and `QueueWorker`, which claims and runs `agent_task` rows |
| `beam_agents` | The agent registry |
| `beam_runtime` | Agent runtime contract and the rig agent-loop driver, with durable run state |
| `beam_gateway` | Model gateway: providers, retry, timeout, circuit breaker, failover, token counting |
| `beam_routing` | `ModelRouter` with modality, capability, cost and fallback strategies; YAML model pools |
| `beam_execution` | Execution control: identity, idempotency, concurrency, leases, cancel, timeout, retry |
| `beam_context` | Context pipeline: sources, policy, composer, envelope and manifest |
| `beam_memory` | Session transcript and summaries |
| `beam_knowledge` | RAG: embedder, vector store (pgvector or JSON), retriever, chunker, ingest |
| `beam_tools` | Tool runtime: allowlist, idempotency, dry run, security stages, MCP, native tools |
| `beam_sandbox` | Path-jailed workspace and working-tree git; protected refs are never pushed |
| `beam_security` | Input, output, RAG, tool, memory and runtime checks (GRL rules on `river_reed`, feature `rules`) |
| `beam_usage` | Usage plans, quota guard, consumption and cost tracking |
| `beam_audit` | Structured audit events and sinks |
| `beam_telemetry` | Tracing, privacy, metrics and failure logs |
| `beam_stream` | Stream transport, chunk merging and durable replay |
| `beam_audio` | Realtime audio session and the STT/TTS pipeline |
| `beam_evals` | Offline evals, qualification gate and feedback |
| `beam_store` | Postgres persistence: the execution hierarchy, catalog, work queue and migrations |

## Persistence

The beam tables live in one Postgres schema: `BEAM_SQL_SCHEMA`, default `riverbeam`. The same database also holds riverbase's tables.

Migrations are diesel migrations in `crates/beam_store/migrations`, run by riverbase's `run_embedded_migrations`. The first one, `0012_baseline`, creates the whole schema on an empty database; later changes are `0013_<name>` onward. The SQL names no schema: the runner creates the schema and puts it first on `search_path`. Diesel's ledger is `<schema>.__diesel_schema_migrations`.

Nothing migrates on startup. Run the migrations first; the server and worker refuse an unmigrated schema, and `GET /health/ready` reports the migration level. Knowledge and memory vectors need the `vector` extension (pgvector), which the baseline creates.

## The showcase

`examples/showcase` is a server with five applications on one beam, switched in the UI's header: a loan desk, a support desk, auto insurance claims, an incident desk and accounts payable. Each has its own personas, agents, tables and routes. See [examples/showcase/README.md](examples/showcase/README.md).

It needs Postgres with pgvector, NATS, Node (for the UI), riverbase's secrets and a model key. The models in `examples/showcase/models.yaml` use NVIDIA NIM and read `NVIDIA_API_KEY`.

```bash
export BEAM_DATABASE_URL=postgres://postgres@127.0.0.1:5432/showcase
export RIVERBASE_DB_URL=$BEAM_DATABASE_URL BEAM_SQL_SCHEMA=showcase
export NATS_URL=nats://127.0.0.1:4222
# riverbase secrets: RIVERBASE_AUTH_APPLICATION_SECRET_KEY,
# RIVERBASE_HOOK_TOKEN_SECRET/_SALT, RIVERBASE_LINK_TOKEN_SECRET/_SALT
export NVIDIA_API_KEY=...
export MODELS_CONFIG_PATH=$PWD/examples/showcase/models.yaml DEFAULT_MODEL=nemotron-lightning
export MCP_SERVERS_CONFIG_PATH=$PWD/examples/showcase/mcp_servers.yaml

cargo run -p showcase --bin showcase-worker -- migrate     # beam schema and the showcases' tables
cargo run -p showcase --bin showcase-worker -- seed        # demo data
(cd examples/showcase && cargo run -p showcase --bin showcase-server -- --migrate-only)   # riverbase's tables
(cd examples/showcase/ui && npm ci && npm run build)       # the UI, served at /showcase

cargo run -p showcase --bin showcase-mcp                   # terminal 1: MCP stubs on 127.0.0.1:9310
(cd examples/showcase && BEAM_EMBEDDED_WORKER=1 cargo run -p showcase --bin showcase-server)  # terminal 2
```

Open <http://127.0.0.1:8097/showcase> (`BEAM_BIND_ADDR`), pick a showcase, then a persona. `BEAM_EMBEDDED_WORKER=1` runs the work queue inside the server; without it, run `cargo run -p showcase --bin showcase-worker` beside the server. Every process claims from the same queue, so a task runs once.

The showcase uses riverbase's mock auth: the `Authorization` header is `MockAuth-` followed by base64url JSON claims. `GET /api/openapi.json` lists every route.

## Streaming

A `stream` or `invoke` command queues the run and returns at once. The run's frames reach the caller while the worker runs it:

- on the caller's riverbase RTC channel, `GET /api/rtc/default~ssestream/{org}.{sub}`, as `beam.stream` events (subscribe before sending commands);
- as a durable replay, `GET /api/beam/sse/{session}/{request}`.

A frame with `is_delta: true` carries the next part of the answer; the last `chunk` holds the whole message, then `done`.

## Security rules

The security checks run as GRL rules on `river_reed`, behind the `rules` feature of `beam_security` (passed on as `beam_core`'s `security-rules` and the showcase's `rules`, all on by default).

- `SECURITY_ENABLED=0` turns the checks off; `SECURITY_ENFORCE=0` keeps them advisory (checked and recorded, never blocking).
- `cargo build -p showcase --no-default-features` builds without the rule engine. That build refuses to start while `SECURITY_ENABLED` is on (`BEM-148`): set `SECURITY_ENABLED=0`. Tenant ownership checks do not use `river_reed` and stay in every build.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `BEAM_DATABASE_URL` | — | Postgres URL for the beam store (`DATABASE_URL` is also read) |
| `BEAM_SQL_SCHEMA` | `riverbeam` | The beam schema, a plain lower-case identifier |
| `RIVERBASE_DB_URL` | — | riverbase's database; the same as `BEAM_DATABASE_URL` |
| `NATS_URL` | `nats://127.0.0.1:4222` | riverbase's bus: work hints and stream frames |
| `BEAM_BIND_ADDR` | `127.0.0.1:8097` | The showcase server's listen address |
| `MODELS_CONFIG_PATH` | — | The model catalog; without it only a built-in dev model is available |
| `SECURITY_ENABLED`, `SECURITY_ENFORCE` | on, on | The security checks (see above) |

## Build and test

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets
```

Tests that need Postgres or NATS skip unless they are configured:

```bash
BEAM_DATABASE_URL=postgres://postgres@127.0.0.1:5432/postgres \
  NATS_URL=nats://127.0.0.1:4222 cargo test --workspace
```

Each test binary uses its own database, `beam_test_<label>`, on that server (schema `ref_beam`), created and migrated on first use.

## Errors

`beam_errors` owns the `BEM-*` catalogue. Raise sites return riverbase errors; HTTP error bodies are RFC 9457 problem details with a public code, and the Rust site code is in `errdata.site_code`.

## License

RFX JSC Proprietary License. See [LICENSE](LICENSE) and [COPYRIGHT](COPYRIGHT).
