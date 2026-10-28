# Status

## Next steps

N1. **Apache Flink connector (native).** Done so far (step 1, see `docs/flink.md`): `arrow-flight` table factory,
    FLIP-27 `FlightSource` (enumerator plans on job start, one split per endpoint, splits pulled on request),
    `FlightSplitReader` via `EndpointReader.forEndpoint` (client-core retries + backup locations), checked
    Arrow -> `GenericRowData` conversion for scalar types, projection pushdown, MiniCluster tests; N1b filter
    pushdown (`FilterTranslator`: only types/operators with identical semantics in Flink and DuckDB) and limit
    pushdown; N1c `FlightCatalog` (`'type' = 'arrow-flight'`, read-only; databases = server schemas, tables
    from `GetTables` with schemas, `ArrowTypeMapping`; catalog tables use the catalog's connection via
    `Catalog.getFactory()`); N1d failover (the split reader tries an endpoint's locations itself, see Log;
    3-node in-process cluster test `TestCluster`); N1e columnar conversion (each Arrow batch copied once into
    Flink heap vectors, rows are `ColumnarRowData` views; checked semantics kept). Remaining, in order:
    - **N1f next:** TLS and Kerberos tests (credentials generated in the job), TPC-H SF0.1 answers vs DuckDB, then perf vs
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

- ~~Which Flink version runs in production?~~ Answered 2026-10-28: production has no Flink; target the most widely deployed
  line - Flink 1.20 LTS first (connector API stable, most installations), then a Flink 2.x build profile.
- Java: `arrow-flight-client-core` is compiled for Java 21, so the Flink cluster must run on Java 21 (Flink 1.20
  supports it as experimental). If production Flink runs on Java 11/17, could the server repo build client core
  with `--release 17`? (maintainer → server repo)
- Client core: `Client.openStream(FlightEndpoint)` does not fail over. It returns the stream of the first
  usable location without opening it (`getStream` is lazy, a dead host only fails on `next()`), and
  `EndpointReader` retries that same first location, so backup locations are never tried. The Flink connector now
  tries the locations itself (N1d); other client-core users (the Spark connector?) are likely affected. Fix in the
  server repo: try the next location when opening/the first `next()` fails. (maintainer → server repo)
- `x-flight-location-failover: 1`: the connector now tries locations in order, so it could send this header and the
  planner would also list itself as a backup (today an endpoint on a dead node has only the other non-planner
  nodes as backups; in a 2-node cluster none). Not sent yet, as the header is part of the server contract -
  OK to send it from the Flink connector? (maintainer)
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
- 2026-10-28: N1b done — filter pushdown (`FilterTranslator` -> client-core `Expr`/`SqlRenderer`; FLOAT/DOUBLE,
  TIME, TIMESTAMP_LTZ, CHAR, nanosecond timestamps, LIKE with `_`/inner `%`/backslash stay in Flink) and limit
  pushdown; 34 tests (MiniCluster answers checked against values computed from the generated data, EXPLAIN shows
  `filter=[...]` / `limit=[5]`). CI 40708981574 green.
- 2026-10-28: N1c done — `FlightCatalog` + `FlightCatalogFactory` (`FlightMetadata`: `GetDbSchemas`, `GetTables`
  with schemas, names re-filtered exactly since filters are LIKE patterns), `ArrowTypeMapping` (always nullable;
  unreadable columns left out with a warning), catalog tables carry only `connector`/`table` options. Fixed in
  `ArrowRowConverter`: UINT64 > Long.MAX into DECIMAL was negative, TIME sub-millisecond digits were cut silently
  (now fails). 50 tests (+16). CI 40717147805 green.
- 2026-10-28: N1d done — found that backup locations were never used (client core opens only the first location,
  see Open questions): `FlightSplitReader` now tries a split's locations in order until one returns the first
  batch, `max-retries` + 1 rounds with backoff; no failover once rows were emitted. `TestCluster`: 3 in-process
  nodes in one Hazelcast cluster (wired like the server's test helper, with seeds); test: partial plan has one
  endpoint per node with backups, a node's Flight server killed (membership kept, so it still gets endpoints),
  its splits read from backups, Flink answers unchanged (fails with the old reader). Catalog names: the server
  only serves directory tables named `[A-Za-z_][A-Za-z0-9_]*`, so `FlightCatalog` never quotes them; a keyword
  name (`group`) plans in partial mode unquoted. 53 tests (+3). CI 40731090607 green.
- 2026-10-28: N1e done — `ArrowRowConverter` now builds a `VectorizedColumnBatch` per Arrow batch (Flink heap
  vectors: primitive arrays, one array copy of a VARCHAR/VARBINARY data buffer, unscaled longs for DECIMAL <= 18
  digits with matching scale; DECIMAL/TIMESTAMP as immutable `DecimalData`/`TimestampData`), the split reader emits
  one `ColumnarRowData` per row. Range checks skipped only where the Arrow type makes them unnecessary, and never
  applied to null slots. Chose a heap copy over wrapping the Arrow buffers (planned "transfer + release on
  `recycle()`"): the fetcher queues batches ahead of emission and rows may outlive `recycle()` with object reuse,
  so a copy needs no allocator lifecycle and cannot read freed memory. Perf (`ArrowRowConverterPerfTest`, `perf`
  tag; 8192-row lineitem-like batch, conversion + `RowDataSerializer.toBinaryRow`): 3.4-3.7 M rows/s vs 1.7 M
  rows/s with the step-1 `GenericRowData` converter (local, one run each). 56 tests (+3).
