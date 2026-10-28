# Status

## Next steps

N0. **Bootstrap:** Maven multi-module skeleton, `ci.yml` (push + workflow_dispatch, Maven cache, `scripts/install-kestrel.sh`
    at `kestrel.ref` for `arrow-flight-client-core` and the in-process test server), Checkstyle/SpotBugs as in the server repo.
N1. **Apache Flink connector (native).** `DynamicTableSource` (ScanTableSource) + `Catalog` over the server's metadata: one
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

## Log

- 2026-10-28 (maintainer): repo created; N0-N3.
