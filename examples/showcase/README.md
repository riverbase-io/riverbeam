# Riverbeam showcase

Real applications on riverbeam, on one server. The UI's header picks a showcase, then a persona; each persona signs its requests with riverbase mock auth.

| Showcase | Personas | What happens |
|---|---|---|
| **Loan desk** | Bob, Dana (borrowers), Carol (loan officer), John (admin) | Borrowers apply with `loan_assistant`, which fills the form in their browser, reads a pay stub (`paystub_reader`) and waits for approval to submit. Carol runs `underwriting_precheck`, a step runtime that pulls credit (MCP), decides exceptions with her and commits a packet to git. John manages revisions, evals, usage, traces, holds and erasure. |
| **Support desk** | Mia, Omar (customers), Sam (support agent) | `support_assistant` answers from the help center, looks orders up (MCP, verified by email) and opens tickets. Sam works the queue with `ticket_triage`, which records a triage and issues refunds he approves, never more than the order total. |
| **Claims** | Leo (policyholder), Ava (adjuster) | `claims_intake` takes the first notice of loss with form and upload cards; `damage_reader` reads the damage estimate. Ava runs `claim_review`, a step runtime that checks coverage and fraud signals, asks her to decide, and holds the payout for her approval. |
| **Incident desk** | Kai (on-call SRE), Lin (incident commander) | `incident_responder` reads alerts, metrics and deploys from monitoring (MCP) and the runbooks, and proposes a rollback that Kai approves. Lin has the postmortem written and committed to git. |
| **Accounts payable** | Nora (AP clerk), Raj (controller) | `invoice_reader` reads an uploaded invoice; `ap_match`, a step runtime, matches it against the PO and goods receipt from the ERP (MCP). Exceptions go to Raj, who decides and approves the payment. |

## Run it

You need Postgres with pgvector, NATS on 127.0.0.1:4222, Node for the UI, and an `NVIDIA_API_KEY` for the models. Run every step from the workspace root, in bash.

1. Write `examples/showcase/.env` (gitignored) with fresh riverbase secrets, then add your model key to it:

   ```bash
   for key in RIVERBASE_AUTH_APPLICATION_SECRET_KEY RIVERBASE_HOOK_TOKEN_SECRET RIVERBASE_HOOK_TOKEN_SALT \
              RIVERBASE_LINK_TOKEN_SECRET RIVERBASE_LINK_TOKEN_SALT LOAN_DESK_BUREAU_KEY; do
     echo "$key=$(openssl rand -hex 32)"
   done > examples/showcase/.env
   echo "NVIDIA_API_KEY=<your key>" >> examples/showcase/.env
   ```

2. Set the environment, in every terminal you use below (point the URLs at your Postgres and NATS):

   ```bash
   set -a; . examples/showcase/.env; set +a
   export BEAM_DATABASE_URL=postgres://postgres@127.0.0.1:5432/showcase
   export RIVERBASE_DB_URL=$BEAM_DATABASE_URL BEAM_SQL_SCHEMA=showcase
   export NATS_URL=nats://127.0.0.1:4222 BEAM_BIND_ADDR=127.0.0.1:8097
   export MODELS_CONFIG_PATH=$PWD/examples/showcase/models.yaml DEFAULT_MODEL=nemotron-lightning
   export MCP_SERVERS_CONFIG_PATH=$PWD/examples/showcase/mcp_servers.yaml
   export USAGE_BACKEND=sql AUDIT_SINKS=stdout,sql MEMORY_LONGTERM_ENABLED=1
   export KNOWLEDGE_EMBEDDER=mock KNOWLEDGE_VECTOR_STORE=pgvector
   export LOAN_DESK_SANDBOX_ROOT=$PWD/target/loan-desk-packets INCIDENT_SANDBOX_ROOT=$PWD/target/incident-postmortems
   export AUDIO_STT_BACKEND=mock AUDIO_TTS_BACKEND=mock AUDIO_DEFAULT_AGENT_NAME=loan_assistant
   ```

3. Create the database, migrate it and load the demo data:

   ```bash
   createdb -h 127.0.0.1 -U postgres showcase
   cargo run -p showcase --bin showcase-worker -- migrate
   cargo run -p showcase --bin showcase-worker -- seed
   (cd examples/showcase && cargo run -p showcase --bin showcase-server -- --migrate-only)
   ```

   `migrate` applies the beam schema and the showcases' tables; `--migrate-only` applies riverbase's.

4. Build the UI (served at `/showcase`):

   ```bash
   (cd examples/showcase/ui && npm ci && npm run build)
   ```

5. Then, in two terminals: the outside services' MCP stubs on 127.0.0.1:9310, and the server with every agent in one process (`BEAM_EMBEDDED_WORKER=1`; `--features audio` adds voice):

   ```bash
   cargo run -p showcase --bin showcase-mcp
   ```

   ```bash
   (cd examples/showcase && BEAM_EMBEDDED_WORKER=1 cargo run -p showcase --features audio --bin showcase-server)
   ```

Open http://127.0.0.1:8097/showcase, pick a showcase in the header, then a persona. To run the work queue in its own process instead, leave out `BEAM_EMBEDDED_WORKER=1` and run `cargo run -p showcase --bin showcase-worker` beside the server; every process claims from the same queue, so a task runs once.

## Layout

| Path | What |
|---|---|
| `src/registry.rs` | The `Showcase` trait, `ALL`, the binder every showcase goes through, the catalog route |
| `src/host/` | The wiring they share: the beam, the work queue, the HTTP surface, runs, voice, shutdown |
| `src/kit.rs` | Route and tool helpers, the shared client tools (`request_input`, `request_document`), memory tools, knowledge ingest |
| `src/records.rs` | The new showcases' records in `{schema}.showcase_record` |
| `src/mcp_stub.rs` | Every outside service as a deterministic MCP stub (`showcase-mcp`) |
| `src/aliases.rs` | The deprecated `/invoke`, `/astream`, `/catalog/agents` routes and `/ws/audio` |
| `src/loan_desk/`, `src/support/`, `src/claims/`, `src/incident/`, `src/invoice/` | One module per showcase: personas, agents, tools, stubs, routes |
| `src/bin/` | `showcase-server`, `showcase-worker`, `showcase-mcp` |
| `knowledge/<showcase>/` | Each showcase's documents for RAG |
| `ui/` | The UI (Vite + React): the shell and picker in `App.tsx`, views per showcase in `src/views/<showcase>/` |
| `models.yaml`, `mcp_servers.yaml`, `riverbase.toml` | The model catalog (NVIDIA NIM), the MCP servers, riverbase's settings |

## Adding a showcase

1. A module `src/<key>/` with personas (their own org and user UUIDs), agents, tools and a struct implementing `registry::Showcase`. Tool and agent names are global: pick names no other showcase uses; the binder refuses a duplicate.
2. Its entry in `registry::ALL` and `pub mod <key>;` in `lib.rs`.
3. Its documents in `knowledge/<key>/`, its MCP stub (if any) in `mcp_stub::router` and `mcp_servers.yaml`.
4. Its views in `ui/src/views/<key>/index.tsx` (keyed by persona role) and an entry in `ui/src/views/index.tsx`.
5. A test in `tests/<key>.rs` that runs its main flow on a scripted model.

The first bind of an agent records its active revision in the database. When you later change an agent's tools or instructions, the next start records the change as a draft revision, and the active one keeps running: activate the draft (the loan desk admin's Revisions tab, or `activate-revision`), or start over with a fresh database (`DROP DATABASE showcase`, then step 3 again).

Security: `SECURITY_ENABLED=0` turns the checks off and `SECURITY_ENFORCE=0` makes them advisory (checked and recorded, never blocking):

```bash
(cd examples/showcase && SECURITY_ENFORCE=0 BEAM_EMBEDDED_WORKER=1 cargo run -p showcase --features audio --bin showcase-server)
```

NVIDIA retires models; when one answers 410, pick a live id from `https://integrate.api.nvidia.com/v1/models` and change `models.yaml`.
