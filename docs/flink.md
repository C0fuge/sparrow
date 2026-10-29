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
and the subtasks read the splits in parallel. A split's locations are tried in order (primary, then the
backup locations) until one returns the first batch, in up to `max-retries` + 1 rounds with exponential backoff
between rounds; once rows were emitted a failure fails the read (a new stream would replay them).
Selected columns, filters and `LIMIT` are pushed into the server SQL (see below).

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
| `tls.truststore` | | Truststore (JKS or PKCS12) with the certificate to trust instead of the JVM's trust anchors; its first certificate entry is used. Needs `tls` and `tls.verify` true; an unreadable store is an error when the table is created |
| `tls.truststore.password` | (empty) | Password of `tls.truststore` |
| `default-schema` | | Schema for unqualified names |
| `kerberos.service-principal` | | Server principal (`flight/host@REALM`); enables Kerberos (SPNEGO). Needs `tls`; not together with `password`/`token` |
| `connect-timeout` | 30 s | Connect timeout |
| `max-retries` | 3 | Retries of failed calls: planning; opening a split (one round over all its locations per try) |

## Security

- TLS: `'tls' = 'true'`. Endpoints advertised by the server carry their scheme (`grpc+tls`); a plaintext
  endpoint location in a TLS plan is refused (credentials are never sent in clear).
- Bearer token or user/password: sent on every call, to the planning node and to the nodes serving endpoints.
- Kerberos: the connector takes the Hadoop login user of the task manager (Flink's
  `security.kerberos.login.keytab` + `security.kerberos.login.principal`, or a ticket cache), exchanges a
  service ticket for `kerberos.service-principal` with the planning node for a signed session, and reads all
  endpoints with that session (the server nodes share the signing secret). Hadoop must be on the Flink
  classpath with `hadoop.security.authentication = kerberos`.
- Tested in the MiniCluster against two-node TLS clusters (generated certificate) with the server's bearer and
  Kerberos authenticators and an in-process KDC (`FlightTlsIntegrationTest`, `FlightKerberosIntegrationTest`).

## Correctness: TPC-H

`TpchIntegrationTest` runs the 22 TPC-H queries (validation parameters, `src/test/resources/tpch`) through
Flink SQL over `arrow-flight` catalog tables of a two-node cluster in partial mode and compares every answer
with DuckDB's on the same Parquet files (same rows in the same order, numbers up to a relative 1e-9). The
data (SF 0.1, 600 k lineitems) comes from `TpchData`: DuckDB SQL with the TPC-H schema, cardinalities, key
relations and value domains, filled with deterministic hash-based values (DuckDB's `tpch` extension is not
bundled with the JDBC driver and CI should not download it), so the numbers are not the published TPC-H
answers. The query texts are written in SQL both engines accept (`WITH` instead of the Q15 view, `val`
instead of the reserved alias `value`, key columns added to `ORDER BY` where ties are possible).

## Performance: TPC-H vs JDBC

`TpchPerfTest` (`perf` tag, local runs only: `./mvnw -Pperf test -pl sparrow-flink -Dtest=TpchPerfTest
[-Dtpch.sf=1] [-Dtpch.queries=1,6]`) times the 22 queries through Flink SQL (batch mode, parallelism 2) on a
two-node in-process cluster with three sources: the `arrow-flight` catalog (`native`), Flink's JDBC connector
(`flink-connector-jdbc-core` 3.3) with the stock Arrow Flight SQL JDBC driver, one split per table (`jdbc`),
and the same with the five large tables split into two key ranges (`jdbc-part`, `scan.partition.*`). Flink's
JDBC connector has no dialect for `jdbc:arrow-flight-sql:` URLs, so the test registers one
(`FlightSqlJdbcFactory`: DuckDB quoting and `LIMIT`); and the server rejects bound statement parameters,
which the JDBC connector uses for every pushed-down filter and partition bound, so the baseline's driver
(`InliningFlightSqlDriver`) writes them into the SQL text. Every answer of every source is also compared
with DuckDB's, so a run at SF 1 checks the SF 1 answers too. Results go to `target/tpch-perf.txt`; numbers in
`docs/STATUS.md` (Log). First run (SF 1, 4 cores): the full lineitem scan 1.56x faster than single-split
JDBC and 1.27x faster than partitioned JDBC; over the 22 queries 1.06x, as Flink's joins and aggregations
dominate the wall time.

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
