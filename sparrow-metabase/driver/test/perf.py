#!/usr/bin/env python3
"""Speed comparison of the native Sparrow Metabase driver with the JDBC route (CI job `metabase-driver`, only when
dispatched with `metabase_perf`).

Runs after smoke.py in the same Metabase (log in as its user) with both plugins loaded: `sparrow` (native, Arrow
Flight SQL client) and `sparrow-jdbc` (the Arrow Flight SQL JDBC driver under Metabase's `sql-jdbc`, SQL compiled by
the native driver, so both send the server the same SQL). Adds the server once per driver, waits for the sync of
`perf.lineitem` (SmokeServer with perf rows), then runs each question through Metabase's API, alternating the drivers:
one warm-up and RUNS timed runs each. Answers of the two drivers must be equal (JSON rows normalized; CSV downloads
compared as sorted lines), and the row count of `perf.lineitem` must be the one the server was started with. Prints a
Markdown table of wall times (median, min) and appends it to $GITHUB_STEP_SUMMARY when set.

Usage: perf.py <metabase-url> <flight-port> <lineitem-rows> [runs]
"""

import hashlib
import json
import os
import re
import statistics
import sys
import time
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/")
PORT = int(sys.argv[2])
ROWS = int(sys.argv[3])
RUNS = int(sys.argv[4]) if len(sys.argv) > 4 else 5
SESSION = {}
DRIVERS = ["sparrow", "sparrow-jdbc"]


def call(method, path, body=None, raw=False):
    """Calls the Metabase API; returns the decoded JSON answer, or the body bytes if `raw`."""
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(BASE + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if "id" in SESSION:
        req.add_header("X-Metabase-Session", SESSION["id"])
    try:
        with urllib.request.urlopen(req, timeout=1800) as resp:
            content = resp.read()
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read().decode()[:2000]}")
    if raw:
        return content
    text = content.decode()
    return json.loads(text) if text else None


def check(label, ok, detail=""):
    """Prints a check result and exits on failure."""
    print(("OK   " if ok else "FAIL ") + label + ("" if ok else f": {detail}"), flush=True)
    if not ok:
        sys.exit(1)


def normalize(value):
    """A JSON result value comparable across drivers: numbers rounded, midnight datetimes as dates."""
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, (int, float)):
        return round(float(value), 4)
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d\d-\d\dT00:00:00(\.0+)?(Z|[+-]00:00)?", value):
        return value[:10]
    return value


def run_json(body):
    """Runs a query through /api/dataset; returns its normalized rows (fails on a query error)."""
    result = call("POST", "/api/dataset", body)
    if result.get("status") != "completed":
        raise SystemExit(f"query failed: {json.dumps(result)[:3000]}")
    return [[normalize(v) for v in r] for r in result["data"]["rows"]]


def run_csv(body):
    """Downloads a query result as CSV; returns (header, number of rows, digest of the sorted rows)."""
    lines = call("POST", "/api/dataset/csv", {"query": body, "format_rows": False}, raw=True).decode().splitlines()
    if not lines:
        raise SystemExit("empty CSV download")
    rows = sorted(lines[1:])
    return lines[0], len(rows), hashlib.sha256("\n".join(rows).encode()).hexdigest()


def add_database(engine):
    """Adds the server as a database of `engine`, waits for its sync; returns (id, lineitem table metadata)."""
    db_id = call("POST", "/api/database", {
        "engine": engine, "name": f"perf {engine}",
        "details": {"host": "localhost", "port": PORT, "user": "test-user", "token": "sparrow-test"},
    })["id"]
    deadline = time.time() + 600
    while time.time() < deadline:
        meta = call("GET", f"/api/database/{db_id}/metadata")
        table = next((t for t in meta["tables"] if (t["schema"], t["name"]) == ("perf", "lineitem")), None)
        if table and table.get("fields") and call("GET", f"/api/database/{db_id}").get(
                "initial_sync_status") == "complete":
            return db_id, table
        time.sleep(3)
    raise SystemExit(f"sync of {engine} did not finish: {[(t['schema'], t['name']) for t in meta['tables']]}")


def questions():
    """(name, kind, body builder taking (db_id, table, field ids by name)); kind is 'json' or 'csv'."""
    q1 = ("SELECT l_returnflag, l_linestatus, sum(l_quantity) AS sum_qty, sum(l_extendedprice) AS sum_base_price,"
          " sum(l_extendedprice * (1 - l_discount)) AS sum_disc_price,"
          " sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, avg(l_quantity) AS avg_qty,"
          " avg(l_extendedprice) AS avg_price, avg(l_discount) AS avg_disc, count(*) AS count_order"
          " FROM perf.lineitem WHERE l_shipdate <= DATE '1998-09-02'"
          " GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus")

    def native(sql):
        return lambda db, t, f: {"database": db, "type": "native", "native": {"query": sql}}

    def mbql(q):
        return lambda db, t, f: {"database": db, "type": "query",
                                 "query": dict({"source-table": t["id"]}, **q(f))}

    def field(name):
        return lambda f: ["field", f[name], None]

    return [
        ("count", "json", mbql(lambda f: {"aggregation": [["count"]]})),
        ("TPC-H Q1 (native)", "json", native(q1)),
        ("count by ship month", "json", mbql(lambda f: {
            "aggregation": [["count"]], "breakout": [["field", f["l_shipdate"], {"temporal-unit": "month"}]]})),
        ("revenue by ship mode, 1994", "json", mbql(lambda f: {
            "aggregation": [["sum", field("l_extendedprice")(f)]], "breakout": [field("l_shipmode")(f)],
            "filter": ["between", field("l_shipdate")(f), "1994-01-01", "1994-12-31"]})),
        ("distinct parts", "json", mbql(lambda f: {"aggregation": [["distinct", field("l_partkey")(f)]]})),
        ("2000 rows (table view)", "json", mbql(lambda f: {
            "filter": ["=", field("l_shipmode")(f), "AIR"],
            "order-by": [["asc", field("l_orderkey")(f)], ["asc", field("l_linenumber")(f)]]})),
        ("CSV 100k rows, 5 columns", "csv", native(
            "SELECT l_orderkey, l_linenumber, l_extendedprice, l_shipdate, l_comment FROM perf.lineitem"
            " WHERE l_orderkey <= 25000")),
        ("CSV 1M rows, all columns", "csv", native(f"SELECT * FROM perf.lineitem LIMIT {min(ROWS, 1_000_000)}")),
    ]


def main():
    props = call("GET", "/api/session/properties")
    if props.get("has-user-setup"):
        SESSION["id"] = call("POST", "/api/session", {"username": "smoke@example.com",
                                                     "password": "Sm0ke-test-pass!"})["id"]
    else:
        SESSION["id"] = call("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {"email": "smoke@example.com", "password": "Sm0ke-test-pass!", "first_name": "Smoke",
                     "last_name": "Test", "site_name": "smoke"},
            "prefs": {"site_name": "smoke", "allow_tracking": False},
        })["id"]

    dbs = {}
    for engine in DRIVERS:
        start = time.time()
        db_id, table = add_database(engine)
        dbs[engine] = (db_id, table, {f["name"]: f["id"] for f in table["fields"]})
        print(f"{engine}: database {db_id} synced in {time.time() - start:.1f} s", flush=True)
        types = {f["name"]: f["base_type"] for f in table["fields"]}
        check(f"{engine}: field types", types.get("l_shipdate") == "type/Date"
              and types.get("l_extendedprice") == "type/Decimal" and types.get("l_shipmode") == "type/Text", types)

    results = []
    for name, kind, build in questions():
        run = run_json if kind == "json" else run_csv
        bodies = {e: build(*dbs[e]) for e in DRIVERS}
        answers = {e: run(bodies[e]) for e in DRIVERS}  # warm-up
        check(f"{name}: same answer", answers["sparrow"] == answers["sparrow-jdbc"],
              (str(answers["sparrow"])[:500], str(answers["sparrow-jdbc"])[:500]))
        if name == "count":
            check("perf.lineitem rows", answers["sparrow"] == [[float(ROWS)]], answers["sparrow"])
        if kind == "csv":
            print(f"     {answers['sparrow'][1]} rows", flush=True)
        times = {e: [] for e in DRIVERS}
        for _ in range(RUNS):
            for e in DRIVERS:
                start = time.perf_counter()
                run(bodies[e])
                times[e].append(time.perf_counter() - start)
        results.append((name, times))
        print(f"     {name}: " + ", ".join(f"{e} {statistics.median(t):.3f} s" for e, t in times.items()), flush=True)

    lines = [f"### Metabase: native driver vs JDBC route ({ROWS} lineitem rows, median of {RUNS} runs)", "",
             "| question | native (s) | JDBC (s) | JDBC / native |", "|---|---:|---:|---:|"]
    for name, times in results:
        n, j = statistics.median(times["sparrow"]), statistics.median(times["sparrow-jdbc"])
        lines.append(f"| {name} | {n:.3f} (min {min(times['sparrow']):.3f}) |"
                     f" {j:.3f} (min {min(times['sparrow-jdbc']):.3f}) | {j / n:.2f}x |")
    report = "\n".join(lines)
    print(report)
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as out:
            out.write(report + "\n")


if __name__ == "__main__":
    main()
