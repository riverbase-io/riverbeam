# Conformance allowlist

Accepted differences between the Python fluvius-beam server (the recording) and the Rust riverbeam server. Every entry needs a reason. `run.py` mirrors the machine-checked part in `GLOBAL_IGNORE` and `INTENDED_DIFFERENCES`; keep both in sync.

## Generated per run (ignored for every target)

| Path | Reason |
|------|--------|
| `**.timestamp`, `**.responded_at`, `**.created_at` | Wall-clock values |
| `**.latency_ms` | Timing |
| `**.request_id`, `**.trace_id` | Minted per request |
| `body.instance` | Problem-details `instance` echoes the concrete request path |
| `headers.date`, `headers.server`, `headers.content-length` | Server and transport details |

Scenarios add their own `ignore` paths for ids they mint (for example `body.errdata.**` where Python's error data carries free text).

## Intended differences (ignored when `--target rust`)

| Path | Python | Rust | Reason |
|------|--------|------|--------|
| `body.type` | `https://fluvius.io/~/py/error/<code>` | `https://riverbase.io/~/rs/error/<code>` | Problem `type` URI base follows each platform; both end in the same `errcode` |
| `body.errdata.site_code` | absent | Rust per-site `BEM-*` code | D3: the public code is on `errcode`; the site code is kept for debugging. Clients must branch on `errcode` only |

## Known differences documented elsewhere

These are not ignored by the harness. They are listed so a failing scenario can be triaged quickly.

| Area | Python | Rust | Reason |
|------|--------|------|--------|
| Security block | `SecurityBlocked` is a plain exception, surfaced as 500 `FLR-001` | 403 `AUT-003` | Rust keeps the correct client-error class. Report to fluvius-beam owners |
| Problem `request_id` | Present | Absent: `riverbase_http` problem details carry none | Riverbase work item |
| `ref_beam.runtime_state` table | Absent | Present | Rust-owned run-state table for rig runs (Phase 04) |
| Lease clock | `lease_expires_at` from the worker clock | From the database clock (`now()`) | Clock skew between workers cannot steal a lease |
| `timeout_at` on resume or retry | `started_at + timeout` (first attempt) | `now + timeout` on every attempt | A resume after a long wait for input is not timed out at once |
| `cancel-execution`, `retry-execution` | No policy (`policy_required = False`) | Casbin `beam.cancel` / `beam.retry` | Rust fails closed |
| `retry-execution` result | `{execution_id, run_id, attempt_no, status}` | `{execution_id, status, result}` | Rust runs the redispatch inline and returns its result |
| `GET /agents` capabilities | Not reported (`AgentQuery` has no such field) | Not reported over HTTP either (the `agent` query has Python's fields); `RiverBeam::agent_capabilities` stays in process | Python parity on the wire |
| Rig approval actions | LangGraph interrupt entries (kind set by the graph) | `beam.approval` entries from `RigRuntime`, same entry shape | Each graph is rewritten on rig (D1); see `graph-inventory.md` |
| Steps of rig runs | One `agent_step` per LangGraph node | One `model` step per turn, one `tools` step per tool batch | rig has no nodes |
| Gateway timeout | 30s on a non-streamed call only | 30s for each next stream item | Rust streams every model call |
| Anthropic `max_tokens` | langchain's default | The model's output budget (`max_output_tokens`, else window / 4) | rig requires the field |
| Google models | OpenRouter only (`sk-or-` key) | OpenRouter key as Python; other keys use native Gemini | D7 requires Gemini |
| Token count of text with a special token | `ValueError` | Counted as text | Never raise while counting |
| In-memory cost ledger | Keeps every event | Keeps the last 1000 | A long-running process must not grow |
| MCP HTTP discovery | Streamable HTTP session | JSON-RPC POSTs (`initialize`, `tools/list`) | The same POST path as calls; no GET SSE behind load balancers |
| MCP non-text result block | Python dict repr | JSON | Rust has no dict repr |
| `knowledge_search` filters | None (the tool has no security context) | Auth context user and org | Never widen past the caller; Phase 08 owns the ACL |
| Envelope history order | Sorted by item id (random message UUIDs), so turns arrive in random order | Source (time) order for history and summary | Python bug; report to fluvius-beam owners |
| Merged transcript summary | System message, trusted, not injection-checked | Untrusted history item, injection-checked | It quotes user turns |
| `MEMORY_BACKEND` default | `in_memory` (no `agent_message` rows) | `sql` over the configured store; `redis` → `sql_cached` | Rust always persisted messages; no Redis (D4) |
| SQL transcript writes | One transaction, no lock | One transaction under a per-session advisory lock | Dense, unique sequences under concurrent writers |
| PDF text | `pypdf` | `pdf-extract` | Compare after whitespace normalization |
| Knowledge store without pgvector | In-memory fallback (vectors lost on restart) | `SqlJsonVectorStore` on `beam_vector_json` | Never lose data silently |
| Non-UUID owner ids in SQL stores | Insert fails (database error) | `BEM-240` before the insert | Same outcome, a clear error |
| `agent_memory.version`, `confidence` | Column defaults; values live in metadata | Columns follow the metadata | Python reads metadata either way |
| Ingest `org_id` | Not in the payload | The caller's org, set by the API (a client's `org_id` is dropped) | Python's org-scoped documents are unreachable |
| `source_url`-only ingest | `TypeError` (500) | `BEM-004` (400), refused by the command itself since plan 019 (before, the command queued and the worker failed) | Beam never fetches URLs for ingest |
| `KNOWLEDGE_EMBEDDER` default | `mock` | HTTP embedder with an API key, else `hash` | Rust's existing default |
| Local Hugging Face embedder | In process (`sentence-transformers`) | HTTP sidecar (text-embeddings-inference), same embedder name | D8 |
| Visibility across stores | In-memory and JSON stores use `record_is_visible`; the pgvector SQL is stricter (all owner columns, NULL scope hidden) | The same per store; tested with Python's matrix | Python's stores differ; Rust keeps each |
| JSON store scope | Not stored (owned scopes without an owner id read back as public) | Kept in `metadata_json.knowledge_scope` | Never widen access |
| Memory recall with a non-UUID user | pgvector query fails | No hits | Fail closed without an error |
| Agent-scoped memory recall on pgvector | Only that agent's rows (`agent_id = :agent`); memories saved without an agent are never recalled by agent runs | That agent's rows and the user's rows without an agent | Same as `record_is_visible` and the in-memory store; `remember-memory` usually saves no agent |
| Ingest dedupe | Scope, agent, user, body hash | Also the org | Rust accepts `org_id`; orgs never share documents |
| Auto-remember | No memory-write check | `BEFORE_MEMORY_WRITE` first | Python's hook is never defined |
| Session ownership | Domain `record_session` only (`BEM-412`) | Also the invoke pipeline: another user's session `BEM-412`, another org's `BEM-411` | A direct invoke cannot write into another caller's session |
| Security engine error | The invoke raises | Deny verdict (fails closed); a KB that fails to load stops startup | Never allow on error |
| `require_approval` verdicts | `ApprovalHandler` marks `hitl_required`; no block | The same: only a deny blocks | Python parity, noted because plan 005 asked for an interrupt |
| HITL resume without a user | `IAM-001` deny | The same; tests give `sample_request` a user | Python parity |
| `HitlConfig.enabled` default | `True`, with `auto_approve_tools` | The same (Rust used to default to false) | Python parity |
| `agent_event.org_id`, `agent_security_decision.org_id` | Not written (null) | The invoke's org | Optional RLS checks these columns |
| MCP token (plan 020) | The command mints it and stores it in `agent_task`/`invoke_request` until it expires | The worker mints it per MCP call from the run's ids; it is never queued or stored (a host-set token is stripped from the stored request) | No credential at rest, in WAL or in backups |
| `agent_security_decision` session, interaction, user (plan 020) | rc8 writes `session_id` and `interaction_id` | Rust writes them too, plus `user_id` from the session (migration 0011), so the decision is deleted with the user and scoped to its owner in the `agent_security_decision` query resource | User-data deletion and the inspector find decisions by session |
| `traceparent` ids | Any three-dash string | W3C hex (32 and 16 digits), else ignored | The trace id is stored on the run |
| Tool audit events | `TOOL_APPROVAL` from the tool executor | Not emitted yet | Follow-up: `beam_tools` has no audit recorder |
| OTLP export | `BEAM_OTLP_ENDPOINT` | Deferred; spans stay in process (10k kept) | Phase 09 outcome |
| Security decision write failure | Raised | Logged; the verdict stands | An audit outage never becomes a security verdict |
| Optional RLS | No connection sets `beam.org_id` (enabling `rls.sql` breaks Python) | `BEAM_RLS_ENABLED` sets it per connection from the task's org | Rust is ready for `rls.sql`; Python is not |
| RLS policy shape (rc8, plan 020) | `NULLIF(current_setting('beam.org_id', true), '')::uuid`: a connection without an org sees no rows | The same `rls.sql`, regenerated from rc8, plus Rust's `agent_task`: no rows instead of an error | Both fail closed; rc8's shape is the reference |
| Schema against rc8 (plan 020) | rc8 dropped `beam_vector_json`; a fresh rc8 database indexes `agent_action.tool_name` and `expires_at` | `beam_vector_json` kept; the two indexes left out of 0010 (`scripts/schema_diff.py` `ALLOWED`) | The JSON-vector fallback needs its table; building the indexes locks a live `agent_action` (plan 020 D5) |
| Reconcile lock | Redis `SET NX` helper, never called (reconcile runs unlocked) | Postgres advisory lock on `hashtext('beam:reconcile:lock')`, held on a connection outside the pool | Rust reconcilers run one at a time; every step is a compare-and-set either way |
| `delete_user_agent_data` | Soft delete (`_deleted`) of sessions, audit, outbox, letters, artifacts; messages stay | Hard delete in one transaction, foreign-key order (messages, runs, steps, actions, executions, events too); usage and cost kept | Rust reads do not filter `_deleted`; a privacy delete must remove the data |
| Legal holds on delete | The worker passes no hold book; holds live in process | Durable in `agent_legal_hold` (plan 019): `place-legal-hold` / `release-legal-hold` (Rust-only, `beam-admin`); `delete-user-agent-data` and the worker both refuse a held user (`BEM-433`) | Python's holds exist but are not wired, and a process-local book cannot stop another process's deletion |
| `started` frame | Not a `StreamChunk`: publish fails, the outbox retries it to death | Stored, marked sent without a live publish | Replay still shows it |
| Error frame | Recorded on dispatch errors; published when streaming | Recorded whenever an execution exists; published when streaming | Replay sees every failure |
| Dead-letter retry/discard | In-memory status only | Status and attempts persisted | Survives restarts |
| Dead-letter JSON | `id, task_name, payload, error, attempts, status` | Adds `execution_id`, `error_code` when set | Additive |
| Dead-letter payload (plan 031) | The failed invoke's request, redacted by key and token prefix (inferred from the port) | `execution_id` and `session_id` only; retry reloads the request from the execution | A secret in the message text is not copied into the letter |
| Dead letters for security denials (plan 031) | Every failed invoke is a dead letter (inferred from the port) | A `BEM-061` denial is not | A retry would be denied again |
| Unknown dead letter | `KeyError` (500) | `BEM-488` (public `BEM-404`) | A clear not-found |
| Readiness | SQL and Redis pings, schema constant | SQL ping, migration ledger, draining; no NATS ping (a NATS outage answers commands `503 BEM-435`). Since plan 020 `schema_version` is rc8's marker (26 once migration 0010 is applied) and the ledger is in `detail.migration` / `detail.latest_migration`; a missing earlier migration reads as `behind` | D4: no Redis |
| `reconcile_now` `retrying` | In-memory controller count | Executions moved by this pass | Rust keeps no in-memory execution cache |
| Coalescing locks | One lock per stream, idle TTL eviction | One buffer map lock | `BEAM_STREAM_LOCK_IDLE_TTL_SECONDS` is N/A |
| Wire trim (plan 020) | rc1 serves `set-canary` and `export-session`, and has the qualification gate, online eval sampling, eval suites, metrics and the release bundle | Removed, as rc8 removed them: the two commands answer 404, a queued `set_canary`/`export_session`/`evaluate_online` task fails with `BEM-091`; `RiverBeam::set_canary`/`export_session` stay as library calls | rc8 is the reference |
| Canary pick for a new execution | `pick_for_new(agent)` without an id: a random draw | Bucketed by `execution_id % 100` (Python's function with the id) | Stable per execution; the same split |
| Revision persistence | `persist_all` inserts new rows and sets `active_revision_id` only | Writes rows and agents changed here (status, timestamps, compatibility, canary columns); a fresh worker's binds take the stored ids and statuses; lifecycle calls reload first | A restart never loses a retire, an activation gate or a canary |
| Changed agent at restart | The new bind is stored `ACTIVE` and becomes the primary (`persist_all` writes every bound agent's `active_revision_id`) | The new bind is stored `DRAFT`; the stored primary keeps new executions until `activate-revision` | Stored traffic wins over a restart's bind (row above) |
| `activate-revision` | The worker activates without `as_primary`: the status changes, the primary does not | The worker activates as primary | Without it a restart's draft could never take traffic |
| Spec timeout on a resume | `started_at + timeout_s` (the first attempt's clock) | From the resumed attempt's start | Rust's controller never times out a resume by its first attempt |
| Pinned checkpoint versions | Overlaid on the shared revision row at restore | Read from the execution row at resume | The revision row keeps its own versions |
| Pin mismatch with the store | Restored per invoke; a mismatch raises | The stored pin replaces the worker's | Workers share the database |
| `MIGRATABLE` migrated ref | Kept on the invoke | Stored on the execution | A later resume reads the migrated ref |
| Unbound agents' revision | `pin_revision(name, runtime="langgraph")` | The same name and `langgraph`, so hashes match | Python parity |
| Context deadline from a spec | `now + narrowed_timeout` | The same | Python parity |
| `compatible_adapter_versions` | In memory | In memory (no column) | Python parity |
| Evals | In memory; `run_evaluation` invokes by agent name (traffic picks the revision) | The same; feedback is also written to `agent_feedback` | Python's feedback query reads the table |
| Unknown eval dataset | `ValueError` | `BEM-494` (public `BEM-400`) | A client error |
| Release bundle | Default rows cite Python's tests | Same format and validator; rows are supplied (Rust evidence at cutover) | Phase 13 |
| Production profile | Python's checks incl. Redis, CORS, mock embedder | Store, records, transcript, security, `full_debug`, durable runtime state | D4; CORS and embedders are the app's (Phase 12) |
| Command authorization | Most commands `policy_required = False` | Casbin on all 28 (plan 006): `beam-user` runs the 17 user commands, the rest need `beam-admin` | Rust fails closed. Scenarios mark these `rust_differs`; roll out with the policy CSV pre-populated |
| Query authorization and ownership | `policy_required = False`; no owner filter (client filters only) | Casbin `<resource>.query`; every query filtered to the caller's user, org, session or execution; another caller's item is not found, `404` `QRY-101` (plan 007 D1) | Never return another user's rows |
| Query row identifier | `_id` in rows and filters | `id` in rows and filters (riverbase's `binding source`; plan 007) | Riverbase's query shape |
| `type` columns in queries | `type` | `action_type`, `document_type`, `memory_type`, `message_type`, `suggestion_type` (plan 007 D3) | `type` is a reserved word in the generated Rust |
| Query and command rows (plan 008) | No `_tenant`; messages, runs, steps, actions, interactions, suggestions, executions and feedback have no `user_id` | Rows carry `_tenant` (the row's org); session-owned rows carry their session's `user_id` and `org_id` (`org_id` filled from the session when the writer has none) | Riverbase's entity shape; the owner filter reads the row |
| Rows of a removed session | Listable | Still listable by their owner and admins (plan 007's views hid them) | `remove-session` soft-deletes the session only, as in Python |
| `agent_feedback` query | Any caller | The caller's own feedback only, never shared by user | One user's ratings and comments |
| `has` filter | Python's contains match | Riverbase's: every `%` and `_` the client sends is escaped (plan 007 D4) | A filter value is matched literally |
| `.usage` | Any authenticated caller | Also the Casbin row `.query` on the domain | Same users in the example policy |
| Missing `Authorization` with mock auth | The example's provider injects a template user | Anonymous (`401`) | riverbase's mock provider; a request names its user |
| Duplicate agent profile | `BEM-411` with status 400 (the code is also `TenantAccessError`'s 403) | `BEM-400` (site `BEM-499`), status 400 | Codes keep one status class |
| Agent profile ownership | Create (ids from the payload), update and remove any profile | The caller's profiles only: create takes the caller's user, and its profile when the session has one (another's ids `BEM-412`); a `profile_id` that is not a UUID is `BEM-400` | Never widen access |
| Forwarded command targets | Any id reaches the worker | A `beam-user`'s execution (`cancel`, `retry`, feedback), memory (`forget`, `edit`, `expire`) or session (`export`) must be its own (`BEM-411` / `BEM-412`; unknown `BEM-404`); `remember-memory` only for itself; `beam-admin` is not limited | Never widen access |
| Forwarded payloads | `model_dump` of the declared fields | The same fields, plus the caller's `org_id` (the worker's RLS scope; ingested documents' org); a client's `org_id` is dropped | A client cannot choose another org's scope |
| Store errors in the domain | Exception text in the response | `FLR-001` without the database's message (logged) | No SQL or data on the wire |
| Pending routes | `/api/auth/mcp-token`, `/api/auth/profiles`, `/api/auth/sign-up`, `/api/auth/switch-profile/{profile_id}`, `/api/auth` (riverbase: `/api/auth/`), `/api/media*` | Not served (`tests/wire_compat/http/openapi_pending.json`) | Riverbase work items: MCP token route (DECIDE-12a), the auth module's profile routes, media routes |
| Run frames on RTC (plan 009) | `/api/rtc/default~ssestream/{channel}`; beam streams on per-run channels | Riverbase's RTC bridge is mounted. Every run frame is also published on the session owner's channel `ws.channel.{org}.{user}` as `{type: "beam.stream", topic: "{session}:{request}", payload: <frame>}`; beam's RTC gate lets a caller reach only `GET /api/rtc/default~ssestream/{own org}.{own sub}` (riverbase's own ACL allows any `ws.channel.*`); `~websocket`, `.meta` and `.info` are refused | Riverbase's RTC bridge has no hook to authorize channels, and riverbase is not changed |
| `/astream/{session}/{request}` (plan 009) | Live frames from Redis, history included | The durable replay of this request's run first (from `Last-Event-ID`; never another run's), then live frames until `done` or `error`; a finished or waiting run's replay ends the stream | NATS keeps no history; the replay covers a late subscriber |
| Deprecated aliases | Not served | `/invoke`, `/astream`, `/astream/{session}/{request}`, `/catalog/agents` for one release, `Deprecation: true`; a principal whose roles the beam policy allows (the alias checks it: riverbase's Casbin layer skips public routes), then the domain commands | Phase 12 change 5 |
| `research-references` | Queues `invoke_research_references`, which no worker registers | Removed (plan 006 D9) | Never implemented by a Rust worker |
| SSE replay access | The session's user, or any caller in its org | The same | Python parity, noted because session queries are user-scoped |
| Audio WebSocket without auth | Socket closed before accept (refused handshake) | `403` before the upgrade, also for a principal whose roles may not `invoke` | Same outcome; Rust checks the role |
| Audio backends | faster-whisper, gTTS, kokoro-onnx in process | The same keys select OpenAI-compatible HTTP sidecars (`AUDIO_STT_URL`, `AUDIO_TTS_URL`); `mock` and `auto` as Python | D8 |
| Audio `auto` without speech-to-text | Asks to install faster-whisper | Asks to set `AUDIO_STT_URL` | D8 |
| Audio agent failure | `[Agent error] <exception text>` | `[Agent error] <public error text>` | No internal detail on the wire |
| Audio decode failure text | Asks to install PyAV | `Audio decode failed: <error>` | No PyAV |
| Example agents | `agent` (LangGraph, one model call) | `agent` on rig with the same preamble, plus echo agents `assistant` and `kb-assistant` for runs without a model | D1; the echo agents keep the smoke test model-free |
| Example security policy | `ToolPolicy.allowed_agents`, `risk_level`, `block_threshold`, `flag_threshold` set | Not ported | Python never reads them |

## Plan 006: commands queue their work

Plan 006 (D5) no longer holds Rust to Python's command responses. The scenarios that exercise them are kept as smoke tests only.

| Area | Python | Rust | Reason |
|------|--------|------|--------|
| Agent runs (`invoke`, `stream`, `resume`) | `invoke` returns the agent's result; `stream` and `resume` return the ids | All three record the session, queue the run and return `{task_id, execution_id, request_id, session_id, agent_name, status: "queued"}`; a repeated `idempotency_key` returns the first run with `status: "existing"`. The deprecated `/invoke` alias still waits for the result (`202` with the ids past `BEAM_INVOKE_WAIT_SECONDS`) | No command waits for a worker; runs survive a missing worker |
| Worker commands (memory, knowledge, revisions, evals, operations) | The worker's result | `{task_id, task, status: "queued"}` plus ids assigned for the client (`memory_id`, `document_id`); read the outcome from the `agent_task` query | Same |
| `cancel-execution` | Worker result | A queued run is cancelled in place (`status: "cancelled"`); a started one queues the cancel | Same |
| `list-revisions`, `list-dead-letters` | Commands | Removed: query `agent_revision` and `agent_dead_letter` (admins only) | Reads are queries |
| Command payloads | Pydantic models | riverbase typed payloads (garde, schemars): a missing or mistyped field is riverbase's payload error | Typed commands |
