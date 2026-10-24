# Cross-stack conformance (plan 005)

Scenarios run the same HTTP requests against the Python fluvius-beam server and the Rust riverbeam server, then diff the results. Python is the reference: its responses are **recorded**, never written by hand.

## Layout

- `scenarios/*.json`: one scenario per file (format below).
- `recorded/*.json`: responses recorded from the Python server. Commit them with the scenario.
- `ALLOWLIST.md`: accepted differences, with reasons.
- `run.py`: the runner. Standard library only.

## Scenario format

```json
{
  "description": "What the scenario proves.",
  "phase": "01",
  "rust_pending_until": "12",
  "steps": [
    { "method": "POST", "path": "/api/fluvius-beam/invoke:exec/agent/{agent_id:no-such-agent}",
      "headers": {}, "body": { "session_id": "{uuid:session}" } }
  ],
  "ignore": ["body.detail"]
}
```

- Templates: `{agent_id:NAME}` is Fluvius `UUID_GENF(NAME)`, `{uuid:LABEL}` is one random UUID per label per run, and `{env:VAR}` reads an environment variable.
- `rust_pending_until`: the phase that delivers the Rust side. Until then a Rust mismatch prints `PENDING` instead of failing.
- `rust_differs`: an intended difference recorded in `ALLOWLIST.md`, as `{"reason": …, "status": …, "errcode": …}`. A Rust mismatch prints `ALLOWED` when the last response has that status and `errcode`, and fails otherwise.
- `ignore`: extra paths to skip, on top of the global rules in `ALLOWLIST.md`. `*` matches one path segment, `**` any number.

Responses are compared after dropping `null`, `{}` and `[]` values, which is the same rule as the Rust wire tests.

## Running

1. Start the Python reference server from `fluvius-beam` (port 8010 by default; it needs its Postgres and Redis):

   ```bash
   PYTHONPATH="./src:./lib" uv run python -m beam_app.server
   ```

2. Record, from the riverbeam repo root: `just conformance-record URL=http://localhost:8010`
3. Start the Rust server (`just run-server`) and check: `just conformance URL=http://localhost:8080`
4. `just conformance-self-test` checks the runner itself without any server.

Scenarios that need a caller send `Authorization: {env:BEAM_CONFORMANCE_AUTH}`: set it to a `MockAuth-<base64url(claims)>` header for a `beam-user` with an `organization_id` on both servers. The Phase 12 scenarios (`cmd_*`, `query_*`) are written by `scripts/gen_conformance_scenarios.py`. Pass other headers with `--header 'name: value'` (via `ARGS=…` on the `just` recipes).
