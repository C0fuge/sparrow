# Native connectors for an Arrow Flight SQL server

Connectors that read from an Arrow Flight SQL server natively (Arrow batches end to end, parallel endpoints, pushdown),
without a JDBC/ODBC layer: Apache Flink (table source and catalog), a native Metabase driver. Status: `docs/STATUS.md`; Flink usage: `docs/flink.md`; Metabase driver: `docs/metabase.md`.

Build: JDK 21, Maven; `scripts/install-kestrel.sh` installs the server's client library at the commit in `kestrel.ref`,
then `./mvnw verify`. License: Apache-2.0.
