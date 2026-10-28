# Flink connector (`sparrow-flink`)

Native Arrow Flight table source and catalog for Flink SQL / Table API (Flink 1.20). Status and backlog: `STATUS.md`.

```sql
CREATE TABLE orders (o_orderkey BIGINT, o_totalprice DECIMAL(15, 2), o_orderdate DATE)
WITH (
  'connector' = 'arrow-flight',
  'host' = 'flight.example',
  'port' = '8815',
  'user' = 'me',
  'token' = '...',
  'table' = 'tpch.orders'        -- schema.table, or a SELECT statement
);
```

How it reads: when the job starts, the split enumerator (job manager) plans the query on the server
(`GetFlightInfo`); every returned endpoint becomes one split (in `partial` result mode one per server node),
and the subtasks read the splits in parallel. Each endpoint's locations are tried in order (primary, then the
backup locations) by the client core when a stream is opened. Selected columns, filters and `LIMIT` are pushed
into the server SQL (see below).

## Catalog

```sql
CREATE CATALOG flight WITH (
  'type' = 'arrow-flight',
  'host' = 'flight.example',
  'port' = '8815',
  'user' = 'me',
  'token' = '...',
  'default-database' = 'tpch'    -- a server schema; defaults to 'default-schema'
);
SELECT o_orderdate, sum(o_totalprice) FROM flight.tpch.orders GROUP BY o_orderdate;
```

Read-only: databases are the server schemas (Flight SQL `GetDbSchemas`), tables and their columns come from
`GetTables` with the table schemas (the columns of `SELECT *`). The catalog's tables are read with the same
source as DDL tables (same pushdown), over the catalog's connection: their options are only
`connector` and `table`, credentials stay in the catalog. Catalog options: the connection options below plus
`default-database`.

Column types (all columns nullable): BOOL -> BOOLEAN; INT8..INT64 -> TINYINT..BIGINT; unsigned integers -> the next
wider signed type, UINT64 -> DECIMAL(20, 0); FLOAT/DOUBLE; DECIMAL(p, s) with p <= 38; UTF8 (all variants) ->
STRING; BINARY -> BYTES, FIXED_SIZE_BINARY(n) -> BINARY(n); DATE; TIME -> TIME(0)/TIME(3); TIMESTAMP -> TIMESTAMP(p)
with p = 0/3/6/9 by unit, with a time zone TIMESTAMP_LTZ(p). Columns of other types (lists, structs, maps,
intervals, DECIMAL with p > 38, HALF_FLOAT) are left out of the table, with a warning in the log.

## Pushdown

- Projection: only the selected columns are read.
- Filters: column-vs-literal comparisons (`=`, `<>`, `<`, `<=`, `>`, `>=`), `IN` lists, `IS [NOT] NULL`, bare
  BOOLEAN columns, `LIKE 'abc%'` / `'%abc'` / `'%abc%'` (no other wildcards, no backslash, no `ESCAPE`), and
  `AND` / `OR` / `NOT` of those, on TINYINT..BIGINT, DECIMAL, VARCHAR/STRING, BOOLEAN, DATE and TIMESTAMP
  (microsecond literals) columns. Everything else - FLOAT/DOUBLE (NaN ordering differs between Flink and the
  server), TIME, TIMESTAMP_LTZ, CHAR, binary types, casts, functions, column-vs-column - stays in Flink, so
  answers are the same with and without pushdown.
- Limit: `LIMIT n` is appended to the server query; Flink keeps its own limit on top (in `partial` mode each
  node may return up to `n` rows).

## Options

| Option | Default | Meaning |
|---|---|---|
| `host` | (required) | Server host |
| `port` | 8815 | Server port |
| `table` | (required) | `schema.table` or a `SELECT` statement (wrapped as a sub-query) |
| `user` | | User name (basic auth, or the owner of a token) |
| `password` | | Password (basic auth); not together with `token` |
| `token` | | Bearer token |
| `tls` | false | Connect with TLS |
| `tls.verify` | true | Verify the server certificate |
| `default-schema` | | Schema for unqualified names |
| `kerberos.service-principal` | | Server principal; enables Kerberos (SPNEGO) |
| `connect-timeout` | 30 s | Connect timeout |
| `max-retries` | 3 | Retries of failed calls (planning, opening a stream) |

## Types

Columns are matched by position with the declared Flink schema. Supported: BOOLEAN, TINYINT, SMALLINT, INT,
BIGINT, FLOAT, DOUBLE, DECIMAL, CHAR/VARCHAR/STRING, BINARY/VARBINARY/BYTES, DATE, TIME, TIMESTAMP (server
type without time zone) and TIMESTAMP_LTZ (server type with time zone). The server may send narrower integer
types than planned; values are widened. A value the declared type cannot hold (integer overflow, decimal
fraction digits beyond the declared scale, a double read as FLOAT, a time-zone mismatch) fails the job — the
connector never changes values silently (this includes a TIME value with sub-millisecond digits: Flink's TIME
holds milliseconds). Other types are rejected when the table is created.

## Limits (current)

- Bounded source. Run it in batch mode: a Flight stream cannot be resumed at a row offset, so restoring a
  streaming checkpoint taken while a split was half read fails with an explicit error instead of
  duplicating or losing rows.
- The connector jar needs Java 21 on the Flink cluster (the server's client core is built for Java 21).
