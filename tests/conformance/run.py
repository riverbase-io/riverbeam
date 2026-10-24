"""Cross-stack conformance harness (plan 005).

Scenarios in ``scenarios/*.json`` describe HTTP requests. Expected results are
not written by hand: ``record`` runs the scenarios against the Python
fluvius-beam server and stores what it returned under ``recorded/``. ``check``
runs them against any target (usually the Rust server) and diffs the result
with the recording, after normalization and the ignore rules in ``ALLOWLIST.md``.

Standard library only, so it runs without a project environment::

    python3 -I tests/conformance/run.py record --url http://localhost:8010
    python3 -I tests/conformance/run.py check  --url http://localhost:8080 --target rust
    python3 -I tests/conformance/run.py self-test
"""

from __future__ import annotations

import argparse
import fnmatch
import http.server
import json
import os
import sys
import threading
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any

HERE = Path(__file__).resolve().parent
SCENARIOS = HERE / "scenarios"
RECORDED = HERE / "recorded"

# Fluvius `fluvius.data` UUID5_NAMESPACE; `UUID_GENF(name)` uses it.
FLUVIUS_UUID5_NAMESPACE = uuid.UUID("f853f38d-83f8-4ef4-9e1b-12b9e27bc1ad")

# Paths ignored in every scenario: values that are generated per run. Keep in
# sync with ALLOWLIST.md. `*` matches one path segment, `**` any number.
GLOBAL_IGNORE = [
    "**.timestamp",
    "**.responded_at",
    "**.created_at",
    "**.latency_ms",
    "**.request_id",
    "**.trace_id",
    "body.instance",
    "headers.date",
    "headers.server",
    "headers.content-length",
]

# Rust and Python differ here by design; see ALLOWLIST.md.
INTENDED_DIFFERENCES = [
    "body.type",  # problem `type` URI base: riverbase.io/~/rs vs fluvius.io/~/py
    "body.errdata.site_code",  # Rust keeps its per-site code (D3)
]


# --- templating ---------------------------------------------------------------


class Template:
    """Expands `{agent_id:NAME}`, `{uuid:LABEL}` and `{env:VAR}` in strings."""

    def __init__(self) -> None:
        self._uuids: dict[str, str] = {}

    def expand(self, value: Any) -> Any:
        if isinstance(value, str):
            return self._expand_str(value)
        if isinstance(value, list):
            return [self.expand(v) for v in value]
        if isinstance(value, dict):
            return {k: self.expand(v) for k, v in value.items()}
        return value

    def _expand_str(self, text: str) -> str:
        out = []
        i = 0
        while i < len(text):
            start = text.find("{", i)
            if start < 0:
                out.append(text[i:])
                break
            end = text.find("}", start)
            token = text[start + 1 : end] if end > start else ""
            kind, _, arg = token.partition(":")
            if end < 0 or kind not in ("agent_id", "uuid", "env") or not arg:
                out.append(text[i : start + 1])
                i = start + 1
                continue
            out.append(text[i:start])
            out.append(self._resolve(kind, arg))
            i = end + 1
        return "".join(out)

    def _resolve(self, kind: str, arg: str) -> str:
        if kind == "agent_id":
            return str(uuid.uuid5(FLUVIUS_UUID5_NAMESPACE, arg))
        if kind == "uuid":
            return self._uuids.setdefault(arg, str(uuid.uuid4()))
        value = os.environ.get(arg)
        if value is None:
            raise SystemExit(f"scenario needs environment variable {arg}")
        return value


# --- running ------------------------------------------------------------------


def send(base_url: str, step: dict, headers: dict[str, str], timeout: float) -> dict:
    url = base_url.rstrip("/") + step["path"]
    body = step.get("body")
    data = None if body is None else json.dumps(body).encode()
    req_headers = {"accept": "application/json", **headers, **step.get("headers", {})}
    if data is not None:
        req_headers.setdefault("content-type", "application/json")
    request = urllib.request.Request(url, data=data, method=step.get("method", "GET"), headers=req_headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as resp:
            status, resp_headers, raw = resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        status, resp_headers, raw = err.code, dict(err.headers), err.read()
    text = raw.decode("utf-8", "replace")
    try:
        parsed: Any = json.loads(text) if text else None
    except json.JSONDecodeError:
        parsed = text
    kept = {k.lower(): v for k, v in resp_headers.items() if k.lower() in ("content-type",)}
    return {"status": status, "headers": kept, "body": parsed}


def run_scenario(scenario: dict, base_url: str, headers: dict[str, str], timeout: float) -> list[dict]:
    template = Template()
    return [send(base_url, template.expand(step), headers, timeout) for step in scenario["steps"]]


# --- comparison ---------------------------------------------------------------


def normalize(value: Any) -> Any:
    """Drop null, {} and [] recursively (same rule as the Rust wire tests)."""
    if isinstance(value, dict):
        out = {}
        for key, item in value.items():
            item = normalize(item)
            if item is None or item == {} or item == []:
                continue
            out[key] = item
        return out
    if isinstance(value, list):
        return [normalize(v) for v in value]
    return value


def flatten(value: Any, prefix: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            out.update(flatten(item, f"{prefix}.{key}" if prefix else str(key)))
        return out
    if isinstance(value, list):
        out = {}
        for i, item in enumerate(value):
            out.update(flatten(item, f"{prefix}.{i}"))
        return out
    return {prefix: value}


def _match_segments(pattern: list[str], path: list[str]) -> bool:
    if not pattern:
        return not path
    if pattern[0] == "**":
        return any(_match_segments(pattern[1:], path[i:]) for i in range(len(path) + 1))
    return bool(path) and fnmatch.fnmatchcase(path[0], pattern[0]) and _match_segments(pattern[1:], path[1:])


def ignored(path: str, patterns: list[str]) -> bool:
    """Segment-wise glob: `*` matches within one segment, `**` matches zero or more segments."""
    parts = path.split(".")
    return any(_match_segments(pattern.split("."), parts) for pattern in patterns)


def diff(expected: list[dict], actual: list[dict], patterns: list[str]) -> list[str]:
    problems = []
    if len(expected) != len(actual):
        return [f"step count {len(actual)} != recorded {len(expected)}"]
    for i, (exp, act) in enumerate(zip(expected, actual)):
        flat_exp = flatten(normalize(exp))
        flat_act = flatten(normalize(act))
        for path in sorted(set(flat_exp) | set(flat_act)):
            if ignored(path, patterns):
                continue
            if flat_exp.get(path, "<missing>") != flat_act.get(path, "<missing>"):
                problems.append(
                    f"step {i}: {path}: expected {flat_exp.get(path, '<missing>')!r}, got {flat_act.get(path, '<missing>')!r}"
                )
    return problems


# --- commands -----------------------------------------------------------------


def load_scenarios(only: list[str]) -> list[dict]:
    items = []
    for path in sorted(SCENARIOS.glob("*.json")):
        scenario = json.loads(path.read_text())
        scenario.setdefault("name", path.stem)
        if only and scenario["name"] not in only:
            continue
        items.append(scenario)
    return items


def parse_headers(values: list[str]) -> dict[str, str]:
    out = {}
    for value in values:
        key, _, val = value.partition(":")
        out[key.strip().lower()] = val.strip()
    return out


def cmd_record(args: argparse.Namespace) -> int:
    RECORDED.mkdir(exist_ok=True)
    for scenario in load_scenarios(args.only):
        result = run_scenario(scenario, args.url, parse_headers(args.header), args.timeout)
        (RECORDED / f"{scenario['name']}.json").write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
        print(f"recorded {scenario['name']}: " + ", ".join(str(r["status"]) for r in result))
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    failed = 0
    for scenario in load_scenarios(args.only):
        name = scenario["name"]
        recorded = RECORDED / f"{name}.json"
        if not recorded.exists():
            print(f"NORECORD {name} (run `record` against the Python server first)")
            failed += 1
            continue
        actual = run_scenario(scenario, args.url, parse_headers(args.header), args.timeout)
        patterns = GLOBAL_IGNORE + scenario.get("ignore", [])
        if args.target == "rust":
            patterns += INTENDED_DIFFERENCES
        problems = diff(json.loads(recorded.read_text()), actual, patterns)
        pending = scenario.get("rust_pending_until")
        differs = scenario.get("rust_differs")
        if not problems:
            print(f"PASS     {name}")
        elif args.target == "rust" and differs:
            last = actual[-1] if actual else {}
            body = last.get("body") if isinstance(last.get("body"), dict) else {}
            got = (last.get("status"), body.get("errcode"))
            want = (differs.get("status"), differs.get("errcode"))
            if got == want:
                print(f"ALLOWED  {name} ({differs.get('reason')})")
            else:
                failed += 1
                print(f"FAIL     {name} (allowed difference expects {want}, got {got})")
        elif args.target == "rust" and pending:
            print(f"PENDING  {name} (Rust surface lands in phase {pending})")
        else:
            failed += 1
            print(f"FAIL     {name}")
            for line in problems:
                print(f"         {line}")
    return 1 if failed else 0


def cmd_self_test(_: argparse.Namespace) -> int:
    """Run record + check against a local stub server; exits non-zero on any mismatch."""
    replies = {"/health/live": (200, {"status": "ok", "timestamp": "t1"})}

    class Stub(http.server.BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            status, body = replies.get(self.path, (404, {"errcode": "BEM-404"}))
            payload = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("content-type", "application/json")
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_: Any) -> None:
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    scenario = {"name": "stub", "steps": [{"method": "GET", "path": "/health/live"}]}
    first = run_scenario(scenario, url, {}, 5)
    replies["/health/live"] = (200, {"status": "ok", "timestamp": "t2"})
    same = run_scenario(scenario, url, {}, 5)
    replies["/health/live"] = (503, {"status": "down"})
    changed = run_scenario(scenario, url, {}, 5)
    server.shutdown()
    checks = [
        ("ignored fields do not fail", diff(first, same, GLOBAL_IGNORE) == []),
        ("real differences fail", len(diff(first, changed, GLOBAL_IGNORE)) == 2),
        ("** does not match a key suffix", not ignored("body.first_timestamp", ["**.timestamp"])),
        ("** matches nested and top-level keys", ignored("body.data.timestamp", ["**.timestamp"]) and ignored("timestamp", ["**.timestamp"])),
        ("trailing ** matches the parent", ignored("body.errdata", ["body.errdata.**"]) and ignored("body.errdata.a.b", ["body.errdata.**"])),
        ("* stays within one segment", not ignored("body.a.b", ["body.*"])),
        ("agent_id template matches UUID_GENF", Template().expand("{agent_id:agent-calculator}") == "4b9616c7-b32f-5dfc-b5dd-54021301f2d7"),
    ]
    for label, ok in checks:
        print(f"{'ok  ' if ok else 'FAIL'} {label}")
    return 0 if all(ok for _, ok in checks) else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("record", "check"):
        p = sub.add_parser(name)
        p.add_argument("--url", required=True, help="base URL of the server under test")
        p.add_argument("--header", action="append", default=[], help="extra header, `name: value`")
        p.add_argument("--timeout", type=float, default=30.0)
        p.add_argument("--only", action="append", default=[], help="scenario name (repeatable)")
        if name == "check":
            p.add_argument("--target", choices=("python", "rust"), default="rust")
    sub.add_parser("self-test")
    args = parser.parse_args()
    return {"record": cmd_record, "check": cmd_check, "self-test": cmd_self_test}[args.command](args)


if __name__ == "__main__":
    sys.exit(main())
