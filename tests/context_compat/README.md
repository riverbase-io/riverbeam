# Context pipeline parity fixtures

Each file in `fixtures/` is one scenario for Python's `ContextBuilder`: the
inputs (agent input, model, spec, history, knowledge and memory hits, tool
schemas, summary, resources) and what Python produced (policy revision,
manifest decisions, envelope messages, token counts, citations).
`crates/beam_context/tests/golden.rs` replays the inputs through the Rust
builder and compares every expected key.

The files are written by `scripts/gen_context_fixtures.py` only; never edit
them by hand. Key order matters (tool schemas are token-counted as JSON text),
so the script does not sort keys. Regenerate from the riverbeam repo root:

```bash
<fluvius-beam venv>/bin/python -I scripts/gen_context_fixtures.py --src ../fluvius-beam/src --src ../fluvius-py/src
```

History ids in the fixtures are sequential, so Python's id-sorted envelope
order equals time order; see the Phase 07 outcome for the order deviation.
