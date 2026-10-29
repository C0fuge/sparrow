# Flink connector (`sparrow-flink`)

Native Arrow Flight table source and catalog for Flink SQL / Table API: Flink 1.20 LTS (default build) and
Flink 2.x (`-Pflink2`, Flink 2.3.0; same sources, the full test set runs against both in CI). Status and backlog: `STATUS.md`.

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

## Deployment

`./mvnw package` builds `sparrow-flink/target/sparrow-flink-<version>-shaded.jar` (about 32 MB; for Flink 2.x
`./mvnw -Pflink2 package` builds `...-shaded-flink2.jar`, use the jar built for the cluster's line): the connector,
the server's client core and their dependencies, with Arrow, gRPC, Netty, Protobuf/Guava/Gson (`com.google`),
Jackson, BouncyCastle and Apache Commons relocated under `sparrow.flink.shaded`, so they cannot clash with the
versions in Flink, Hadoop or other connectors. Flink itself and slf4j are not in the jar (the cluster provides
them; `flink-connector-base` comes with `flink-dist`). Put it into Flink's `lib/`, or load it per session:

```sql
ADD JAR '/path/to/sparrow-flink-0.1.0-SNAPSHOT-shaded.jar';
CREATE CATALOG flight WITH ('type' = 'arrow-flight', ...);
```

The jar has no native code: TLS uses the JDK's provider (netty-tcnative is left out) and Netty uses NIO (no
epoll). Netty system properties are relocated with the classes (`sparrow.flink.shaded.io.netty.*`). The cluster
must run on Java 21 (client core) with the `--add-opens` Flink sets by default for Java 17+
(`java.base/java.nio`, needed by Arrow). `ShadedJarIT` (run by `./mvnw verify` after packaging) checks the jar
contents and reads a table and a catalog over TLS from a class path without the connector, the jar added with
`ADD JAR`.

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

## Lookup joins

`arrow-flight` tables (DDL and catalog tables) can be the dimension side of a lookup join:

```sql
SELECT o.order_id, c.c_name
FROM orders AS o
JOIN flight.tpch.customer /*+ OPTIONS('lookup.cache' = 'PARTIAL', 'lookup.partial-cache.max-rows' = '10000',
        'lookup.partial-cache.expire-after-write' = '10 min') */
  FOR SYSTEM_TIME AS OF o.proc_time AS c
  ON c.c_custkey = o.customer_id;
```

For every probe row (or cache miss) the connector plans `SELECT <used columns> FROM <table> WHERE k1 = v1
AND ...` on the server and reads all returned endpoints (in `partial` mode one per node) with the same location
failover as a scan; every matching row is returned, a missing key returns none. Key values are written into
the SQL as literals (the server does not bind statement parameters), so keys are limited to the types whose
equality the server evaluates exactly as Flink: TINYINT..BIGINT, DECIMAL, VARCHAR/STRING, BOOLEAN, DATE and
TIMESTAMP(p <= 6). Other key types (FLOAT/DOUBLE, CHAR, TIME, TIMESTAMP_LTZ, binary) are rejected when the job is
planned. A NULL key value matches no row (SQL equality) and is not sent to the server. Other conditions on the
dimension table (`AND c.c_mktsegment = 'X'`) are evaluated by Flink on the looked-up rows.

Cache: Flink's standard lookup options; no cache by default (every lookup is a round trip: `GetFlightInfo` plus
one `DoGet` per endpoint).

| Option | Default | Meaning |
|---|---|---|
| `lookup.cache` | NONE | `NONE` or `PARTIAL` (`FULL` is not supported) |
| `lookup.partial-cache.max-rows` | | Maximum cached keys; `PARTIAL` needs this or an expiry |
| `lookup.partial-cache.expire-after-write` | | Drop entries this long after they were loaded |
| `lookup.partial-cache.expire-after-access` | | Drop entries this long after their last use |
| `lookup.partial-cache.cache-missing-key` | true | Also cache keys without rows |

On catalog tables, set them with an `OPTIONS` hint as above (credentials stay in the catalog). A cached row
is not refreshed when the server data changes until it expires.

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

- Bounded scan source. Run scans in batch mode: a Flight stream cannot be resumed at a row offset, so restoring
  a streaming checkpoint taken while a split was half read fails with an explicit error instead of
  duplicating or losing rows. Lookups keep no state and work in batch and streaming jobs (both tested), e.g.
  enriching a Kafka stream.
- The connector jar needs Java 21 on the Flink cluster (the server's client core is built for Java 21).
