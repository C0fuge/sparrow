#!/usr/bin/env python3
"""Smoke test of the Sparrow Metabase driver in a running Metabase (CI job `metabase-driver`).

Sets Metabase up, adds the in-process test server (sparrow-metabase SmokeServer) as a `sparrow` database, waits for
the sync, then checks the synced tables and fields and the answers of a native and a GUI-built (MBQL) query.

Usage: smoke.py <metabase-url> <flight-port>
"""

import json
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/")
PORT = int(sys.argv[2])
SESSION = {}


def call(method, path, body=None):
    """Calls the Metabase API and returns the decoded JSON answer."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if "id" in SESSION:
        req.add_header("X-Metabase-Session", SESSION["id"])
    try:
        with urllib.request.urlopen(req, timeout=300) as resp:
            text = resp.read().decode()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()[:2000]}")
    return json.loads(text) if text else None


def check(label, ok, detail=""):
    """Prints a check result and exits on failure."""
    print(("OK   " if ok else "FAIL ") + label + ("" if ok else f": {detail}"), flush=True)
    if not ok:
        sys.exit(1)


def query(body):
    """Runs a query through /api/dataset and returns its rows (fails on a query error)."""
    result = call("POST", "/api/dataset", body)
    if result.get("status") != "completed":
        raise SystemExit(f"query failed: {json.dumps(result)[:3000]}")
    return result["data"]["rows"], [c["name"] for c in result["data"]["cols"]]


def main():
    token = call("GET", "/api/session/properties")["setup-token"]
    SESSION["id"] = call("POST", "/api/setup", {
        "token": token,
        "user": {"email": "smoke@example.com", "password": "Sm0ke-test-pass!", "first_name": "Smoke",
                 "last_name": "Test", "site_name": "smoke"},
        "prefs": {"site_name": "smoke", "allow_tracking": False},
    })["id"]

    engines = call("GET", "/api/session/properties")["engines"]
    check("driver plugin loaded", "sparrow" in engines, sorted(engines))

    db = call("POST", "/api/database", {
        "engine": "sparrow", "name": "sparrow",
        "details": {"host": "localhost", "port": PORT, "user": "test-user", "token": "sparrow-test"},
    })
    db_id = db["id"]

    deadline = time.time() + 300
    tables = {}
    while time.time() < deadline:
        meta = call("GET", f"/api/database/{db_id}/metadata")
        tables = {(t["schema"], t["name"]): t for t in meta["tables"]}
        numbers = tables.get(("sparrow", "numbers"))
        if numbers and numbers.get("fields") and call("GET", f"/api/database/{db_id}").get(
                "initial_sync_status") == "complete":
            break
        time.sleep(3)
    check("tables synced", {("sparrow", "numbers"), ("sparrow", "types"), ("sparrow", "nested")} <= set(tables),
          sorted(tables))
    fields = {f["name"]: f for f in tables[("sparrow", "types")]["fields"]}
    expected = {"id": ("INTEGER", "type/Integer"), "bi": ("BIGINT", "type/BigInteger"),
                "dec": ("DECIMAL(10,2)", "type/Decimal"), "s": ("VARCHAR", "type/Text"),
                "dt": ("DATE", "type/Date"), "ts": ("TIMESTAMP", "type/DateTime"),
                "tstz": ("TIMESTAMP WITH TIME ZONE", "type/DateTimeWithLocalTZ")}
    got = {n: (fields[n]["database_type"], fields[n]["base_type"]) for n in expected if n in fields}
    check("field types synced", got == expected, got)

    rows, cols = query({"database": db_id, "type": "native",
                        "native": {"query": "SELECT count(*) AS c, sum(n) AS s FROM sparrow.numbers"}})
    check("native query", cols == ["c", "s"] and [[int(v) for v in r] for r in rows] == [[100000, 4999950000]],
          (cols, rows))

    numbers_id = tables[("sparrow", "numbers")]["id"]
    m_id = next(f["id"] for f in tables[("sparrow", "numbers")]["fields"] if f["name"] == "m")
    rows, cols = query({"database": db_id, "type": "query",
                        "query": {"source-table": numbers_id, "aggregation": [["count"]],
                                  "breakout": [["field", m_id, None]],
                                  "filter": ["<", ["field", m_id, None], 3]}})
    # n % 7 over 0..99999: residues 0..4 occur 14286 times, 5 and 6 14285 times
    check("MBQL query", [[int(v) for v in r] for r in rows] == [[0, 14286], [1, 14286], [2, 14286]], (cols, rows))


if __name__ == "__main__":
    main()
