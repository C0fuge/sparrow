# Status

## Next steps

N1. **Apache Flink connector (native).** Done so far (step 1, see `docs/flink.md`): `arrow-flight` table factory,
    FLIP-27 `FlightSource` (enumerator plans on job start, one split per endpoint, splits pulled on request),
    `FlightSplitReader` via `EndpointReader.forEndpoint` (client-core retries + backup locations), checked
    Arrow -> `GenericRowData` conversion for scalar types, projection pushdown, MiniCluster tests. Remaining, in order:
    - **N1b next:** filter pushdown (`SupportsFilterPushDown`: translate Flink `ResolvedExpression` comparisons /
      AND / OR / IS NULL / IN / LIKE on columns + literals to client-core `Expr`, render with `SqlRenderer`, pass the
      WHERE to `TableScan.probe`; return unsupported ones as remaining) and limit pushdown (`SupportsLimitPushDown`:
      append `LIMIT n`; Flink keeps its own Limit on top). Tests: generated SQL (unit) + answers equal with and
      without pushdown (MiniCluster).
    - N1c: `FlightCatalog` (+ `CatalogFactory` `arrow-flight`): databases = server schemas, tables from Flight SQL
      `GetTables`/`GetColumns`, Arrow -> Flink type mapping (reuse/extend `ArrowRowConverter` rules).
    - N1d: multi-endpoint + failover test: two in-process servers / partial mode (see how the server's tests run a
      cluster), kill the primary location of an endpoint, answers stay equal.
    - N1e: columnar conversion (Arrow vectors wrapped as Flink `ColumnVector`s / `ColumnarRowData` with an owned,
      transferred batch released on `recycle()`), keep the checked semantics; measure against step 1.
    - N1f: TLS and Kerberos tests (credentials generated in the job), TPC-H SF0.1 answers vs DuckDB, then perf vs
      Flink JDBC + Flight SQL JDBC driver (`perf` tag), connector shading (uber jar), Flink 2.x module.
    Goal (maintainer): `DynamicTableSource` (ScanTableSource) + `Catalog` over the server's metadata: one
    Flink split per Flight endpoint in partial mode (parallel readers, backup locations for failover), Arrow batches
    converted to Flink `RowData` columnar-wise (no per-value boxing where avoidable), projection + filter + limit pushdown
    into the server SQL, auth (token/basic, Kerberos as in the Spark connector), TLS. Flink 1.20 LTS first, then 2.x.
    Tests: Flink MiniCluster against the in-process server, TPC-H SF0.1/SF1 answers equal to DuckDB reading the files.
    Perf: Flink SQL TPC-H SF1 via the connector vs Flink's JDBC connector with the Flight SQL JDBC driver (baseline).
N2. Flink lookup source (dimension joins: `LookupTableSource` with a small cache) and bounded/unbounded semantics notes.
N3. **Metabase native driver**: a Metabase driver (Clojure) that uses the Arrow Flight SQL Java client directly (no
    JDBC), DuckDB SQL dialect, metadata sync from Flight SQL `GetTables`/`GetColumns`; CI in a `metabase/metabase`
    container: sync + native and GUI-built queries, answers checked; speed vs the JDBC-based community driver.

## Open questions

- Which Flink version runs (or will run) in production? (maintainer, pending)
- Java: `arrow-flight-client-core` is compiled for Java 21, so the Flink cluster must run on Java 21 (Flink 1.20
  supports it as experimental). If production Flink runs on Java 11/17, could the server repo build client core
  with `--release 17`? (maintainer → server repo)
- Streaming mode: a bounded source restored from a checkpoint with a half-read split fails (Flight streams are
  not resumable and their row order is not guaranteed). Is batch-only acceptable, or do we need a resumable
  ticket (row offset) from the server? (maintainer)

## Log

- 2026-10-28 (maintainer): repo created; N0-N3.
- 2026-10-28: N0 done — Maven skeleton (BOMs aligned with the server: gRPC 1.84.2, Netty 4.2.19, Jackson 2.21.8;
  without them the in-process server fails on a gRPC mix), Checkstyle as in the server plus missing-Javadoc checks,
  SpotBugs, `ci.yml` (Maven cache + server artifacts cached by ref). CI 40691138388 green. N1 step 1 (table source,
  projection pushdown, 25 tests incl. MiniCluster) — CI 40692468363 green. Tests generate their Parquet with DuckDB
  (server test classpath); partial mode needs `schema.table` names (a `table` option `SELECT ...` works).
