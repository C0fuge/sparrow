# Status

## Next steps

> **Maintainer, 2026-10-29 13:15 - FINISH MODE:** finish N3 (the native Metabase driver already started: sync + queries + CI + speed vs the JDBC driver) and the Flink follow-ups that are already half-done; then write "SPARROW DONE" and stop. No new items.

N1. **Apache Flink connector (native).** Done so far (step 1, see `docs/flink.md`): `arrow-flight` table factory,
    FLIP-27 `FlightSource` (enumerator plans on job start, one split per endpoint, splits pulled on request),
    `FlightSplitReader` via `EndpointReader.forEndpoint` (client-core retries + backup locations), checked
    Arrow -> `GenericRowData` conversion for scalar types, projection pushdown, MiniCluster tests; N1b filter
    pushdown (`FilterTranslator`: only types/operators with identical semantics in Flink and DuckDB) and limit
    pushdown; N1c `FlightCatalog` (`'type' = 'arrow-flight'`, read-only; databases = server schemas, tables
    from `GetTables` with schemas, `ArrowTypeMapping`; catalog tables use the catalog's connection via
    `Catalog.getFactory()`); N1d failover (the split reader tries an endpoint's locations itself, see Log;
    3-node in-process cluster test `TestCluster`); N1e columnar conversion (each Arrow batch copied once into
    Flink heap vectors, rows are `ColumnarRowData` views; checked semantics kept); N1f TLS + bearer + Kerberos
    MiniCluster tests against two-node TLS clusters (`tls.truststore` options, in-process Kerby KDC); N1g
    TPC-H SF0.1: all 22 queries through Flink SQL over catalog tables of a 2-node partial-mode cluster equal
    DuckDB's answers on the same Parquet (`TpchIntegrationTest`, data from `TpchData`); N1h perf vs Flink JDBC +
    Flight SQL JDBC driver and SF 1 answers (`TpchPerfTest`, `perf` tag, see Log); N1i shaded jar
    (`sparrow-flink-<version>-shaded.jar`, relocated deps, `ShadedJarIT` loads it with `ADD JAR`); N1j Flink 2.x
    (`-Pflink2` = Flink 2.3.0, same sources; CI matrix runs every test on 1.20 and 2.3). N1 is complete apart
    from these follow-ups, none blocking:
    - Perf baseline on 2.x: `TpchPerfTest` uses `flink-connector-jdbc-core` 3.3.0-1.20 also under `-Pflink2`
      (it compiles; the `perf` tag was not run on 2.x). Switch to the 2.x JDBC connector release if needed.
    - Open questions below (failover header, batch-only, bound parameters) wait for the maintainer.
    Goal (maintainer): `DynamicTableSource` (ScanTableSource) + `Catalog` over the server's metadata: one
    Flink split per Flight endpoint in partial mode (parallel readers, backup locations for failover), Arrow batches
    converted to Flink `RowData` columnar-wise (no per-value boxing where avoidable), projection + filter + limit pushdown
    into the server SQL, auth (token/basic, Kerberos as in the Spark connector), TLS. Flink 1.20 LTS first, then 2.x.
    Tests: Flink MiniCluster against the in-process server, TPC-H SF0.1/SF1 answers equal to DuckDB reading the files.
    Perf: Flink SQL TPC-H SF1 via the connector vs Flink's JDBC connector with the Flight SQL JDBC driver (baseline).
N2. **Flink lookup source** — done 2026-10-29 (see Log, `docs/flink.md` "Lookup joins"). Follow-ups, none blocking:
    - Async lookups (`AsyncLookupFunctionProvider`) if per-key latency matters; full cache (`lookup.cache` =
      `FULL`, reload of the whole table) if wanted; several keys per round trip (`IN`); all purely additive.
N3. **next:** **Metabase native driver**: a Metabase driver (Clojure) that uses the Arrow Flight SQL Java client directly (no
    JDBC), DuckDB SQL dialect, metadata sync from Flight SQL `GetTables`/`GetColumns`; CI in a `metabase/metabase`
    container: sync + native and GUI-built queries, answers checked; speed vs the JDBC-based community driver.
    Plan and state: `docs/metabase.md`. Done: N3a Java bridge, N3b driver skeleton + CI smoke job, N3c answers
    checked on real queries (date bucketing, joins, distinct/percentile, string filters, native template tags;
    see Log), N3d speed comparison (see Log), N3e `regex`, `datetime-diff`, `convert-timezone` (see Log).
    Next step (finish mode): the Flink 2.x perf-baseline follow-up of N1, then "SPARROW DONE".

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
- Client core and BouncyCastle: `arrow-flight-client-core` depends on bcpkix/bcutil 1.85 (truststore -> PEM)
  but a consumer with Hadoop 3.5 on the classpath resolves bcprov 1.82 and fails with `NoClassDefFoundError:
  IANAObjectIdentifiers` as soon as `tls.truststore` is used. The connector pins bcprov 1.85 (as the server
  does); client core could declare bcprov-jdk18on 1.85 itself. Also `Configuration(host, port, truststore,
  ...)` only logs a warning and trusts nothing when the store cannot be read; the connector checks it and
  fails at table creation. (maintainer → server repo)
- `x-flight-location-failover: 1`: the connector now tries locations in order, so it could send this header and the
  planner would also list itself as a backup (today an endpoint on a dead node has only the other non-planner
  nodes as backups; in a 2-node cluster none). Not sent yet, as the header is part of the server contract -
  OK to send it from the Flink connector? (maintainer)
- Bound statement parameters: `CreatePreparedStatement` works, but a query with `?` fails ("Statement
  parameters (?) are not supported; put the values into the SQL text"). Flink's JDBC connector binds every
  pushed-down filter value and partition bound, so stock Flink JDBC (and BI tools that bind parameters) cannot
  read from the server; the perf baseline inlines them in a wrapper driver (`InliningFlightSqlDriver`). Does the
  server want to support `DoPut` parameter binding for prepared statements? (maintainer → server repo)
- Default result mode and table-less SQL: without `x-flight-result-mode` the server plans in partial mode, which
  refuses SQL without exactly one table (`SELECT 1`: "Can only select from ONE table"); `CreatePreparedStatement`
  refuses it in any mode. Generic JDBC clients (Metabase's `sql-jdbc` connection test, pool test queries) fail on
  this. Should the server answer such queries in complete mode on its own? Not blocking: the native driver always
  sends `complete` (maintainer → server repo)
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
  rows/s with the step-1 `GenericRowData` converter (local, one run each). 56 tests (+3). CI 40746810605 green.
- 2026-10-28: N1f done — TLS and auth tested end to end: `TestCluster` serves TLS (advertises `grpc+tls`
  locations) with the server's own authenticators (test helper `ServerAuthenticators` in the server package);
  certificate generated per run (BouncyCastle), Kerberos against an in-process Kerby KDC with generated
  principals/keytabs, Flink cluster logging in from the client keytab (`security.kerberos.login.*`; without it
  the MiniCluster's delegation-token manager tries a ticket cache and the job fails). Two-node clusters: both
  nodes' endpoints read over TLS with the token / the Kerberos session (shared signing secret); wrong token,
  untrusted certificate, plaintext client and unknown service principal fail the job. New options
  `tls.truststore`/`tls.truststore.password` (validated at table creation); Kerberos without TLS or with a
  password/token is now a validation error. bcprov pinned to 1.85 (see Open questions). 68 tests (+12).
  CI 40765700528 green.
- 2026-10-28: N1g done — TPC-H SF0.1 correctness: `TpchData` writes TPC-H-shaped data with DuckDB SQL
  (schema, cardinalities, key relations, dbgen value domains and word lists, deterministic hash values; the
  `tpch` extension is not bundled in the DuckDB JDBC jar and CI should not download it), large tables in 4
  files; `TpchIntegrationTest` runs the 22 queries (validation parameters, one dialect for both engines, key
  tie-breakers in `ORDER BY`) through Flink over an `arrow-flight` catalog on a 2-node `TestCluster` (lineitem
  endpoints on both nodes asserted) and compares ordered rows with DuckDB on the same files: all equal (DOUBLE
  avg/quotients in DuckDB vs Flink DECIMAL(…, 6) compared at Flink's scale). Found: with `-ea` Calcite's
  Volcano planner fails Q18/Q21 with its internal "has wrong best cost" check (no statistics involved; not
  active in production) - Calcite assertions now disabled in surefire (`-da:org.apache.calcite...`).
  ~90 s for the 22 queries locally. 91 tests (+23). CI 40781931010 green.
- 2026-10-29: N1h done — `TpchPerfTest` (`perf` tag, local only): 22 TPC-H queries + a full lineitem scan
  through Flink SQL (batch, parallelism 2) on a 2-node in-process cluster, sources `native` (catalog, partial
  mode), `jdbc` (Flink `flink-connector-jdbc-core` 3.3.0-1.20 + Arrow Flight SQL JDBC driver 19.0.0, one split)
  and `jdbc-part` (large tables in 2 key ranges); every answer compared with DuckDB's, so it also checks SF 1:
  all 69 answers equal. Needed for the baseline: a test JDBC dialect for `jdbc:arrow-flight-sql:`
  (`FlightSqlJdbcFactory`) and `InliningFlightSqlDriver`, since the server rejects bound parameters (Open
  questions). SF 1, local 4 cores, one run, wall s native/jdbc/jdbc-part: scan 17.0/26.5/21.6 (jdbc 1.56x,
  jdbc-part 1.27x slower), 22 queries 253/268/270 (1.06x; Q1 14.1/17.2/16.9, Q9 99.8/102.4/103.2 - joins in
  Flink dominate; the in-process nodes run DuckDB with 1 thread). Flink's SUM(INT) keeps INT and overflows at
  SF 1 (the scan probe casts). 94 tests (+3). CI 40797950700 green.
- 2026-10-29: N1i done — shaded jar `sparrow-flink-<version>-shaded.jar` (maven-shade, ~32 MB): Arrow, gRPC,
  Netty, perfmark, `com.google` (Protobuf, common protos, Guava, Gson, flatbuffers), Jackson, BouncyCastle and
  Apache Commons relocated under `sparrow.flink.shaded`; client core and the connector unrelocated; Flink and
  slf4j left out (`flink-connector-base` is in `flink-dist` via `flink-connector-files`, checked for 1.20.3 and
  2.3.0); no native code (netty-tcnative and the epoll libraries dropped: JDK TLS, NIO), no multi-release
  copies. `ShadedJarIT` (failsafe, after `package`; the main classes directory replaced by an empty one):
  jar holds only connector/client-core/relocated classes, the connector is absent without the jar, and after
  `ADD JAR` a table (with filter/limit pushdown) and a catalog read a 2-node TLS cluster with a token. 94 + 4
  tests. CI 40807395590 green. N1j done — the unchanged sources compile and pass all 98 tests against Flink
  2.3.0 (no deprecation warnings); profile `-Pflink2` (classifier `shaded-flink2`), CI matrix 1.20/2.x.
  CI 40807995648 green (both matrix jobs). Local builds: `~/.wgetrc` needs `ca_certificate=` the proxy CA for `mvnw`'s download.
- 2026-10-29: N2 done — lookup joins: `FlightDynamicTableSource` is a `LookupTableSource`; `FlightLookupFunction`
  plans `SELECT <columns> FROM <table> WHERE <pushed filters> AND k = v ...` per key and reads every endpoint
  (partial mode: all nodes) through `FlightSplitReader` (same location failover), rows copied out of the batch.
  Keys written as literals (no bound parameters, see Open questions), so only exactly comparable key types are
  accepted (as filter pushdown, TIMESTAMP p <= 6; DOUBLE etc. rejected at planning); NULL key values match
  nothing without a server call. Flink's partial cache via the standard `lookup.cache` options (also as
  `OPTIONS` hints on catalog tables; `FULL` rejected); no cache by default. Flink 1.20 keeps dimension
  conditions in the `LookupJoin` (`where=[...]`), applied on the looked-up rows. `FlightLookupIntegrationTest`
  (7: every key type composite, several rows per key, missing/NULL keys, quoted strings, whole-minute
  timestamps, cache hint, rejected DOUBLE key, 2-node partial mode, streaming mode) + 5 unit tests; new
  `sparrow.dims` test table. CI 40819420866 red on 2.x only (test's anonymous `LookupContext` lacked 2.x's
  `preferCustomShuffle()`; a stale incremental local `-Pflink2` run had hidden it - use `clean`), fixed:
  CI 40819937283 green (both matrix jobs). 106 + 4 tests.
- 2026-10-29: N3a done — `sparrow-metabase` Maven module, the Java side of the Metabase driver (design in
  `docs/metabase.md`): `FlightSqlBridge` (connection from Metabase details with the Flink connector's
  validation; sync metadata from `GetDbSchemas`/`GetTables` with exact-name filtering; queries always planned
  with `x-flight-result-mode: complete`, since client core's `callOptions()` always asks for `partial`, which
  would split aggregations per node; reads without a deadline, cancel from another thread), `QueryResult`
  (lazy `Object[]` rows), `ArrowValues` (Arrow -> Java values Metabase handles, timestamps with zone as
  `OffsetDateTime` in the column's zone), `DuckDbTypes` (DuckDB type names for `database_type`). Found: Parquet
  stores DuckDB's unsigned integers as wider signed types (UTINYINT -> SMALLINT, UBIGINT -> DECIMAL(20,0)).
  15 tests (3 unit + 12 against the in-process server). CI 40824250224 green (both matrix jobs).
- 2026-10-29: N3b done — Metabase driver skeleton `sparrow-metabase/driver` (Metabase module layout:
  `deps.edn`, `resources/metabase-plugin.yaml` with the bridge's detail keys, `src/metabase/driver/sparrow.clj`):
  `:sparrow` under `:sql`, `can-connect?`, `describe-database*`, `describe-fields` (DuckDB type -> base type),
  `mbql->native` with `*compile-with-inline-parameters*` (server rejects `?`), `execute-reducible-query` (lazy
  rows, cancel via Metabase's canceled channel; native queries with parameters refused for now), features
  narrowed. The bridge ships as `sparrow-metabase-<version>-shaded.jar` (relocated under
  `sparrow.metabase.shaded`). CI job `metabase-driver`: Metabase checkout at v0.65.2.2, the driver built with
  `clojure -X:build:drivers:build/driver :driver :sparrow` (JDK 25, Clojure CLI 1.12.0.1488; ~45 s, verify
  passes), loaded into the `metabase/metabase:v0.65.2.2` container next to `SmokeServer` (in-process server,
  test sources); `smoke.py`: setup, database added, sync (tables, field types), native aggregation and MBQL
  count/breakout/filter answers. Found: Netty 4.2 disables `sun.misc.Unsafe` on Java 25+ (Metabase's runtime)
  and Arrow's Netty allocator then fails ("Failed to read message.", `ClassCastException` to
  `PooledUnsafeDirectByteBuf`); the bridge sets `io.netty.noUnsafe=false` unless set (relocated in the shaded
  jar, so only the bundled Netty) - Unsafe memory access will be denied by a later JDK, then Arrow needs another
  allocator. Also seen: client core retries SQL errors (e.g. unknown table) 4 times before failing. Local
  pre-check without Metabase: the driver namespace against stub Metabase namespaces + the shaded jar + the
  in-process server, JDK 21 and 25. 16 bridge tests (+1). CI 40830332236 red (Metabase 0.65 has no `engines`
  in session properties), 40830856369 red (the Netty/Unsafe failure), 40831748643 green (all three jobs).
- 2026-10-29: N3c done — Metabase driver answers checked on real queries. `sql.qp/date` for `:sparrow`
  (`date_trunc`/`extract`; DATE results cast back to DATE since DuckDB's `date_trunc` returns TIMESTAMP; TIME
  via `make_time`; TIMESTAMP WITH TIME ZONE bucketed as `timezone('UTC', x)`, since `date_trunc` on it follows
  the server session's zone; weeks start Monday in DuckDB, adjusted to Metabase's start of week), interval
  arithmetic (`INTERVAL 'n unit'`), `now()`, `to_timestamp`. Found: Metabase's default TIMESTAMPTZ literal
  (`timestamp with time zone '... +02:00'`) fails in DuckDB ("Unknown TimeZone '+02:00'") - `inline-value`
  overrides write `TIMESTAMPTZ '...+02:00'` (and microseconds for TIMESTAMP/TIME). Native template tags and
  field filters now substituted as inline literals (the `:sql` substitution with
  `*compile-with-inline-parameters*`) instead of refused; leftover `?` arguments still refused. `regex`,
  `datetime-diff`, `convert-timezone` switched off. Smoke data `shop.orders`/`shop.customers` (`TestServer`),
  expected answers computed in `smoke.py` from the generating formulas (checked locally against DuckDB): 24
  new checks (8 temporal breakouts, date/timestamptz filters, distinct/median/percentile, join, 6 string
  filters, 5 native queries with tags). CI 40843975383 green (all three jobs, 29 smoke checks).
- 2026-10-29: N3d done — speed comparison of the native Metabase driver with the JDBC route. Baseline
  `sparrow-metabase/driver-jdbc` (`:sparrow-jdbc`, perf only, not shipped): `:sql-jdbc` driver over the Arrow
  Flight SQL JDBC driver 19.0.0 whose SQL is compiled by `:sparrow` (same SQL to the server, so only the
  transport differs). Found: the server refuses table-less SQL (`SELECT 1`) in its default (partial) result mode
  and in `CreatePreparedStatement`; the baseline sends `x-flight-result-mode: complete` as a JDBC connection
  property (the JDBC driver forwards unknown properties as call headers) and tests connections with a plain
  statement. `SmokeServer` takes a `perf.lineitem` row count (TPC-H lineitem shape, 6 M rows = SF 1, four
  files); `perf.py` runs 8 questions through Metabase's API on both databases (warm-up answers must be equal),
  median of 5 alternating runs. Only in `workflow_dispatch` runs with `metabase_perf` (both drivers + perf
  step); push runs unchanged. CI 40861785290 green (all three jobs), SF 1, s native/JDBC: count 0.053/0.055,
  TPC-H Q1 (native) 0.350/0.348, count by month 0.344/0.350, sum by ship mode 0.144/0.155, distinct 0.141/0.159,
  2000-row table view 0.156/0.181 (1.15x), CSV 100 k rows 0.572/0.669 (1.17x), CSV 1 M rows x 13 columns
  7.35/8.42 (1.15x). Aggregations are server-bound (equal); row transfer is 13-17 % faster natively (the rest of a large
  download is presumably Metabase's own row processing and CSV writing; not profiled). Earlier dispatches 40859930805,
  40860388753, 40861125149 red (deps.edn insertion, `SELECT 1` refused). Bridge tests 17 (+1, `perf.lineitem`).
- 2026-10-29: N3e done — the Metabase driver now supports `regex` (regexExtract: `regexp_extract` guarded by
  `regexp_matches`, so NULL without a match instead of DuckDB's ''; first capturing group if the pattern has one,
  as Postgres; `regex/lookaheads-and-lookbehinds` off, RE2), `datetime-diff` (Metabase's whole units truncated
  toward zero; months from year/month/day parts as Postgres' `age` - DuckDB's `date_sub('month')` counts Jan 31
  -> Feb 28 as a month, which a smoke sum catches: 2149 vs 2121; week/day from `DATE - DATE`, hour/minute/second
  via `date_sub` on timestamps; TIMESTAMPTZ as UTC wall clock) and `convert-timezone` (`timezone(zone, x)`,
  Postgres semantics). 17 new smoke checks with answers computed in `smoke.py` (both directions of every diff,
  zoneinfo for the zones), SQL cross-checked locally against DuckDB. CI 40877446820 green (all three jobs, 46
  smoke checks). TPC-H-shaped questions in the smoke test left out (N3d's perf run already checks equal answers).
