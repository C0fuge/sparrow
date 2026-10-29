# Metabase driver (`sparrow-metabase`, in progress)

A native Metabase driver for the Arrow Flight SQL server: Metabase talks to the server through the Arrow Flight
SQL Java client (the server repository's client core), with no JDBC layer. Status and backlog: `STATUS.md` (N3).

## Design

Two parts:

1. **Java bridge** (`sparrow-metabase` Maven module, done): everything that touches Arrow, tested with JUnit
   against the in-process server like the Flink connector.
   - `FlightSqlBridge.connect(details)`: a connection from the database's connection details (a map with string
     keys; values may be strings, numbers or booleans, blank values count as absent): `host`, `port` (8815),
     `user`, `password` or `token`, `tls`, `tls-verify` (true), `tls-truststore`, `tls-truststore-password`,
     `kerberos-principal` (TLS only, no password/token), `default-schema`, `max-retries`, `timeout-ms`.
     Inconsistent details fail with `IllegalArgumentException`. Connections come from client core's cache.
   - Sync: `schemas()` (`GetDbSchemas`), `tables(schema)` (`GetTables`; `TableRef` with schema, name, type),
     `columns(schema, table)` (`GetTables` with schemas; `Column` with name, DuckDB type name, position,
     nullability). Names are matched exactly (Flight SQL filters are LIKE patterns).
   - Queries: `execute(sql)` plans the query in **complete** result mode (`x-flight-result-mode: complete`):
     Metabase sends aggregations, joins, ordering and limits, which only the whole data set answers. In a cluster
     whose nodes keep their files apart the server refuses such a plan with an error (never a partial answer).
     The result (`QueryResult`) is an iterator of `Object[]` rows read batch by batch; `cancel()` may be called
     from another thread (Metabase's cancel channel) and cancels the stream on the server; `close()` releases
     it. `describe(sql)` returns the columns without running the query; `ping()` runs `SELECT 1`.
   - Values (`ArrowValues`): BOOLEAN `Boolean`; TINYINT/SMALLINT/INTEGER/UTINYINT/USMALLINT `Integer`;
     BIGINT/UINTEGER `Long`; UBIGINT `BigInteger`; FLOAT `Float`; DOUBLE `Double`; DECIMAL (also HUGEINT)
     `BigDecimal`; VARCHAR `String`; BLOB `byte[]`; DATE `LocalDate`; TIME `LocalTime`; TIMESTAMP(_S/_MS/_NS)
     `LocalDateTime` (stored wall clock); TIMESTAMP WITH TIME ZONE `OffsetDateTime` in the column's zone;
     INTERVAL and nested values (lists, structs, maps) as text (JSON for nested values).
   - Type names (`DuckDbTypes`): DuckDB's names (`BIGINT`, `DECIMAL(10,2)`, `TIMESTAMP WITH TIME ZONE`,
     `INTEGER[]`, `STRUCT("a" INTEGER)`, ...), stored by Metabase as `database_type` and mapped to base types
     by the driver. Parquet keeps unsigned integers as wider signed types, so table columns of unsigned data
     show up as `SMALLINT`/`DECIMAL(20,0)` etc.
2. **Clojure driver** (`sparrow-metabase/driver`, skeleton done): Metabase's module layout (`deps.edn`,
   `resources/metabase-plugin.yaml`, `src/metabase/driver/sparrow.clj`), modelled on Metabase's own non-JDBC SQL
   driver `bigquery-cloud-sdk` (v0.65.2.2). `driver/register! :sparrow :parent #{:sql}`;
   - `can-connect?` (`ping`), `describe-database*` (`tables(null)`: every table of every schema),
     `describe-fields` (`columns` per table, ordered by schema, table, position; DuckDB type name -> base type,
     lists/structs/maps/intervals as `:type/Text` since their values arrive as text, BLOB `:type/*`),
     `db-default-timezone` UTC.
   - `mbql->native`: the `:sql` parent's compiler with `driver/*compile-with-inline-parameters*` bound, so
     values are written into the SQL text (the server rejects bound parameters, see Open questions).
   - `execute-reducible-query`: `FlightSqlBridge.execute`, columns from the planned schema, rows reduced lazily
     batch by batch; a message on Metabase's canceled channel calls `QueryResult.cancel()` and ends the
     reduction; the result is closed when `respond` returns. Native queries with Metabase parameters (template
     tags compile to `?`) are refused with an `unsupported-feature` error for now, never run without values.
   - Features: `describe-fields`; no key constraints, uploads, actions, transforms, routing, impersonation or
     `set-timezone`; the rest as the `:sql` parent.
   - Connection form: host, port (8815), user, password or token, default schema, TLS (verify, truststore and
     its password, Kerberos service principal), planning timeout and retries under advanced options.

   The plugin jar bundles the bridge as the module's shaded jar (`sparrow-metabase-<version>-shaded.jar`:
   Arrow, gRPC, Netty, Protobuf/Guava, Jackson, BouncyCastle and Apache Commons relocated under
   `sparrow.metabase.shaded`, slf4j left to Metabase): Metabase plugins share one class loader, and other
   plugins such as BigQuery bring their own Arrow/gRPC/Netty versions. Arrow needs
   `--add-opens java.base/java.nio`, which Metabase's jar manifest and Docker image already set.

   Build (CI job `metabase-driver`; clojars.org is not reachable from development sessions): check out
   `metabase/metabase` at the pinned tag, copy `deps.edn`, `src`, `resources` to `modules/drivers/sparrow`,
   the shaded jar to its `lib/sparrow-metabase-shaded.jar`, add `metabase/sparrow {:local/root "sparrow"}` to
   `modules/drivers/deps.edn`, then `clojure -X:build:drivers:build/driver :driver :sparrow` (JDK 25, Clojure CLI
   as in Metabase's CI) -> `resources/modules/sparrow.metabase-driver.jar`, which goes into Metabase's plugins
   directory (`MB_PLUGINS_DIR`).

## Tests

- Bridge: `FlightSqlBridgeTest` (details, type names), `FlightSqlBridgeIntegrationTest` (in-process server:
  sync metadata, every value type incl. nulls, unsigned integers, 100 000 rows over several batches in order,
  aggregation answers, nested values, empty results, early close and cancel, SQL errors, unreachable server).
- Driver smoke test (CI job `metabase-driver`, `driver/test/smoke.py`): the in-process server with the bridge
  tests' tables (`SmokeServer`, test sources) and the `metabase/metabase` container of the pinned tag with the
  built plugin; through Metabase's API: setup, the `sparrow` engine is listed, a database is added and synced
  (tables, `database_type`/`base_type` of the fields), a native aggregation and an MBQL count with breakout and
  filter return the expected rows.
- Next: the TPC-H-shaped data and query set with answers checked (native + MBQL incl. date bucketing, which
  needs DuckDB's date functions in `sql.qp`), native query parameters, then a comparison with the JDBC-based
  community driver.
