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
2. **Clojure driver** (next): `modules/drivers/sparrow` in Metabase's layout, modelled on Metabase's own
   non-JDBC SQL driver `bigquery-cloud-sdk` (v0.65.2.2): `driver/register! :sparrow :parent #{:sql}`;
   `can-connect?` (`ping`), `describe-database*` (`tables`), `describe-fields` (`columns`, DuckDB name ->
   base type), `execute-reducible-query` (compile with `sql.qp` + honeysql in DuckDB's dialect, run `execute`,
   reduce the rows, `cancel` on the canceled channel), `database-supports?` per feature, `db-default-timezone`,
   `mbql->native` from `:sql`. Native queries with Metabase parameters are substituted into the SQL text (the
   server rejects bound parameters, see Open questions). The plugin jar bundles the bridge and client core
   (Arrow/gRPC/Netty relocated as in the Flink jar: Metabase plugins share one class loader, and other plugins
   such as BigQuery bring their own Arrow/gRPC/Netty versions).

## Tests

- Bridge: `FlightSqlBridgeTest` (details, type names), `FlightSqlBridgeIntegrationTest` (in-process server:
  sync metadata, every value type incl. nulls, unsigned integers, 100 000 rows over several batches in order,
  aggregation answers, nested values, empty results, early close and cancel, SQL errors, unreachable server).
- Driver (next): CI job that checks out Metabase at a pinned tag, builds the driver with Metabase's build
  scripts, starts a server container/in-process server with TPC-H-shaped data, and runs sync + native and
  MBQL (GUI-built) queries with answers checked; then a comparison with the JDBC-based community driver.
