#!/usr/bin/env python3
"""Smoke test of the Sparrow Metabase driver in a running Metabase (CI job `metabase-driver`).

Sets Metabase up, adds the in-process test server (sparrow-metabase SmokeServer) as a `sparrow` database, waits for
the sync, then checks the synced tables and fields and the answers of native and GUI-built (MBQL) queries: date
bucketing and extraction on DATE, TIMESTAMP and TIMESTAMP WITH TIME ZONE columns, joins, distinct counts,
percentiles, string filters and native queries with template tags. The expected answers of the `shop` tables are
computed here from the formulas that generated them (`TestServer`), not by the server.

Usage: smoke.py <metabase-url> <flight-port>
"""

import datetime
import json
import sys
from collections import Counter, defaultdict
from decimal import Decimal
import time
import urllib.error
import urllib.request

BASE = sys.argv[1].rstrip("/")
PORT = int(sys.argv[2])
SESSION = {}


ORDERS = 1000
CUSTOMERS = 20
CITIES = ["Berlin", "Paris", "Rome", "Oslo"]


def orders():
    """The rows of shop.orders as TestServer generates them."""
    rows = []
    for i in range(ORDERS):
        day = datetime.date(2023, 1, 1) + datetime.timedelta(days=(i * 7) % 730)
        created = datetime.datetime.combine(day, datetime.time()) + datetime.timedelta(seconds=(i * 3637) % 86400)
        note = "50% off" if i % 10 == 0 else "gift_wrap" if i % 10 == 1 else f"Note {i}"
        rows.append({"id": i, "customer_id": i % CUSTOMERS, "order_date": day, "created_at": created,
                     "amount": Decimal((i * 37) % 10000) / 100, "status": ["open", "shipped", "returned"][i % 3],
                     "note": note})
    return rows


def percentile_cont(values, p):
    """SQL percentile_cont: linear interpolation between the closest ranks."""
    v = sorted(values)
    pos = p * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return float(v[lo]) + (pos - lo) * (float(v[hi]) - float(v[lo]))


def sunday_week(d):
    """Start (Sunday) of the week of date `d`: Metabase's default start of week."""
    return d - datetime.timedelta(days=(d.weekday() + 1) % 7)


def as_int_rows(rows):
    """Rows with every value as int."""
    return [[int(v) for v in r] for r in rows]


def as_date_rows(rows):
    """Rows whose first value is a date or datetime (only its date part kept), the rest as int."""
    return [[r[0][:10]] + [int(v) for v in r[1:]] for r in rows]


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

    db = call("POST", "/api/database", {
        "engine": "sparrow", "name": "sparrow",
        "details": {"host": "localhost", "port": PORT, "user": "test-user", "token": "sparrow-test"},
    })
    db_id = db["id"]
    # an unknown engine is refused, so this also shows that the plugin registered the driver
    check("database added", db.get("engine") == "sparrow", db)

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
    check("tables synced", {("sparrow", "numbers"), ("sparrow", "types"), ("sparrow", "nested"), ("shop", "orders"),
                            ("shop", "customers")} <= set(tables), sorted(tables))
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

    shop_checks(db_id, tables)


def shop_checks(db_id, tables):
    """Answers on the shop tables, compared with values computed from the generating formulas."""
    data = orders()
    orders_t = tables[("shop", "orders")]
    customers_t = tables[("shop", "customers")]
    f = {n["name"]: n["id"] for n in orders_t["fields"]}
    c = {n["name"]: n["id"] for n in customers_t["fields"]}

    def mbql(q):
        return query({"database": db_id, "type": "query", "query": dict({"source-table": orders_t["id"]}, **q)})[0]

    def by(unit, key, column="order_date", extra=None):
        """Count of orders by a temporal unit of `column`, rows ordered by the bucket."""
        rows = mbql(dict({"aggregation": [["count"]],
                          "breakout": [["field", f[column], {"temporal-unit": unit}]]}, **(extra or {})))
        expected = sorted(Counter(key(r) for r in data).items())
        return rows, [[k, n] for k, n in expected]

    rows, expected = by("month", lambda r: r["order_date"].replace(day=1).isoformat())
    check("DATE by month", as_date_rows(rows) == expected, (rows[:3], expected[:3]))
    rows, expected = by("week", lambda r: sunday_week(r["order_date"]).isoformat())
    check("DATE by week (Sunday)", as_date_rows(rows) == expected, (rows[:3], expected[:3]))
    rows, expected = by("quarter", lambda r: r["created_at"].date().replace(
        month=(r["created_at"].month - 1) // 3 * 3 + 1, day=1).isoformat(), "created_at")
    check("TIMESTAMP by quarter", as_date_rows(rows) == expected, (rows[:3], expected[:3]))
    rows, expected = by("year", lambda r: f"{r['created_at'].year}-01-01", "created_tz")
    check("TIMESTAMP WITH TIME ZONE by year", as_date_rows(rows) == expected, (rows, expected))
    rows, expected = by("day", lambda r: r["created_at"].date().isoformat(), "created_tz",
                        {"filter": ["<", ["field", f["id"], None], 50]})
    expected = [[k, n] for k, n in sorted(Counter(r["created_at"].date().isoformat()
                                                  for r in data if r["id"] < 50).items())]
    check("TIMESTAMP WITH TIME ZONE by day", as_date_rows(rows) == expected, (rows[:3], expected[:3]))
    rows, expected = by("hour-of-day", lambda r: r["created_at"].hour, "created_at")
    check("TIMESTAMP hour of day", as_int_rows(rows) == expected, (rows[:3], expected[:3]))
    # day of week 1 = Sunday (start of week)
    rows, expected = by("day-of-week", lambda r: (r["order_date"].weekday() + 1) % 7 + 1)
    check("DATE day of week", as_int_rows(rows) == expected, (rows, expected))
    rows, expected = by("month-of-year", lambda r: r["created_at"].month, "created_tz")
    check("TIMESTAMP WITH TIME ZONE month of year", as_int_rows(rows) == expected, (rows, expected))

    rows = mbql({"aggregation": [["count"]], "filter": ["between", ["field", f["order_date"], None],
                                                         "2023-03-01", "2023-03-31"]})
    expected = sum(1 for r in data if datetime.date(2023, 3, 1) <= r["order_date"] <= datetime.date(2023, 3, 31))
    check("DATE between filter", as_int_rows(rows) == [[expected]], (rows, expected))
    rows = mbql({"aggregation": [["count"]], "filter": [">=", ["field", f["created_tz"], None],
                                                         "2024-06-01T12:00:00Z"]})
    expected = sum(1 for r in data if r["created_at"] >= datetime.datetime(2024, 6, 1, 12))
    check("TIMESTAMP WITH TIME ZONE filter", as_int_rows(rows) == [[expected]], (rows, expected))

    rows = mbql({"aggregation": [["distinct", ["field", f["customer_id"], None]],
                                 ["median", ["field", f["amount"], None]],
                                 ["percentile", ["field", f["amount"], None], 0.9]]})
    amounts = [r["amount"] for r in data]
    ok = (int(rows[0][0]) == CUSTOMERS and abs(float(rows[0][1]) - percentile_cont(amounts, 0.5)) < 0.006
          and abs(float(rows[0][2]) - percentile_cont(amounts, 0.9)) < 0.006)
    check("distinct, median, percentile", ok, (rows, percentile_cont(amounts, 0.5), percentile_cont(amounts, 0.9)))

    rows = mbql({"aggregation": [["sum", ["field", f["amount"], None]]],
                 "breakout": [["field", c["city"], {"join-alias": "c"}]],
                 "joins": [{"source-table": customers_t["id"], "alias": "c", "fields": "none",
                            "condition": ["=", ["field", f["customer_id"], None],
                                          ["field", c["id"], {"join-alias": "c"}]]}]})
    sums = defaultdict(Decimal)
    for r in data:
        sums[CITIES[r["customer_id"] % 4]] += r["amount"]
    expected = sorted([k, v] for k, v in sums.items())
    got = [[r[0], Decimal(str(r[1])).quantize(Decimal("0.01"))] for r in rows]
    check("join, sum by city", got == expected, (rows, expected))

    def count_where(flt):
        return int(mbql({"aggregation": [["count"]], "filter": flt})[0][0])

    note, status = ["field", f["note"], None], ["field", f["status"], None]
    # `%` and `_` in the values are literal characters, not LIKE wildcards
    check("contains '_'", count_where(["contains", note, "_"]) == sum(1 for r in data if "_" in r["note"]))
    check("contains '%'", count_where(["contains", note, "%"]) == sum(1 for r in data if "%" in r["note"]))
    check("starts-with, case-insensitive",
          count_where(["starts-with", status, "SHI", {"case-sensitive": False}]) == ORDERS // 3)
    check("starts-with, case-sensitive", count_where(["starts-with", status, "SHI"]) == 0)
    check("ends-with", count_where(["ends-with", note, " 99"]) == 1)
    check("does-not-contain", count_where(["does-not-contain", note, "Note"]) == ORDERS // 5)

    def native(sql, tags, params):
        return query({"database": db_id, "type": "native", "native": {"query": sql, "template-tags": tags},
                      "parameters": params})[0]

    def tag(name, typ, **more):
        return dict({"id": name, "name": name, "display-name": name, "type": typ}, **more)

    rows = native("SELECT count(*) FROM shop.orders WHERE customer_id = {{cid}} AND status = {{st}}"
                  "[[ AND id < {{max_id}}]]",
                  {"cid": tag("cid", "number"), "st": tag("st", "text"), "max_id": tag("max_id", "number")},
                  [{"type": "number/=", "target": ["variable", ["template-tag", "cid"]], "value": [3]},
                   {"type": "category", "target": ["variable", ["template-tag", "st"]], "value": "it's"}])
    check("native number + text tags (quote in value)", as_int_rows(rows) == [[0]], rows)
    rows = native("SELECT count(*) FROM shop.orders WHERE customer_id = {{cid}} AND status = {{st}}",
                  {"cid": tag("cid", "number"), "st": tag("st", "text")},
                  [{"type": "number/=", "target": ["variable", ["template-tag", "cid"]], "value": [3]},
                   {"type": "category", "target": ["variable", ["template-tag", "st"]], "value": "shipped"}])
    expected = sum(1 for r in data if r["customer_id"] == 3 and r["status"] == "shipped")
    check("native number + text tags", as_int_rows(rows) == [[expected]], (rows, expected))
    rows = native("SELECT count(*) FROM shop.orders WHERE order_date >= {{d}}", {"d": tag("d", "date")},
                  [{"type": "date/single", "target": ["variable", ["template-tag", "d"]], "value": "2024-07-01"}])
    expected = sum(1 for r in data if r["order_date"] >= datetime.date(2024, 7, 1))
    check("native date tag", as_int_rows(rows) == [[expected]], (rows, expected))
    rows = native("SELECT count(*) FROM shop.orders WHERE {{created}}",
                  {"created": tag("created", "dimension", dimension=["field", f["created_tz"], None],
                                  **{"widget-type": "date/all-options"})},
                  [{"type": "date/all-options", "target": ["dimension", ["template-tag", "created"]],
                    "value": "2023-03-01~2023-03-31"}])
    expected = sum(1 for r in data if datetime.date(2023, 3, 1) <= r["created_at"].date() <= datetime.date(2023, 3, 31))
    check("native field filter (date range)", as_int_rows(rows) == [[expected]], (rows, expected))
    rows = native("SELECT count(*) FROM shop.orders WHERE {{st}}",
                  {"st": tag("st", "dimension", dimension=["field", f["status"], None],
                             **{"widget-type": "string/="})},
                  [{"type": "string/=", "target": ["dimension", ["template-tag", "st"]],
                    "value": ["open", "returned"]}])
    expected = sum(1 for r in data if r["status"] in ("open", "returned"))
    check("native field filter (several values)", as_int_rows(rows) == [[expected]], (rows, expected))


if __name__ == "__main__":
    main()
