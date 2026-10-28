package sparrow.flink;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.Map;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

/**
 * In-process Arrow Flight SQL server for the tests over Parquet data generated with DuckDB in a temporary
 * directory: {@code sparrow.types} (one row per supported type plus an all-null row) and
 * {@code sparrow.numbers} (100 000 rows, several Arrow batches).
 */
final class TestServer implements AutoCloseable {

    /** Rows of {@code sparrow.numbers}. */
    static final int NUMBERS = 100_000;

    final TestFlightServerHelper helper;
    final int port;
    private final Path dataDir;

    private TestServer(TestFlightServerHelper helper, Path dataDir) {
        this.helper = helper;
        this.dataDir = dataDir;
        this.port = helper.location.getUri().getPort();
    }

    /** Starts a server over freshly generated data. */
    static TestServer start() throws Exception {
        Path dir = Files.createTempDirectory("sparrow-test-db");
        writeTables(dir);
        return new TestServer(TestFlightServerHelper.builder().dataDir(dir.toString()).start(), dir);
    }

    /** Table options of a connector table on this server. */
    Map<String, String> options(String table) {
        return Map.of("connector", "arrow-flight", "host", "localhost", "port", String.valueOf(port),
                "user", "test-user", "token", "sparrow-test", "table", table);
    }

    /** Writes the test tables as {@code <dir>/<schema>/<table>/*.parquet}. */
    private static void writeTables(Path dir) throws Exception {
        Path types = Files.createDirectories(dir.resolve("sparrow/types"));
        Path numbers = Files.createDirectories(dir.resolve("sparrow/numbers"));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("SET TimeZone = 'UTC'");
            s.execute("COPY (SELECT * FROM (VALUES "
                    + "(1, true, 7::TINYINT, 300::SMALLINT, 9000000000::BIGINT, 1.5::FLOAT, 2.25::DOUBLE,"
                    + " 12345.67::DECIMAL(10,2), 'héllo', '\\xCA\\xFE'::BLOB, DATE '2024-02-29',"
                    + " TIME '13:45:30.123', TIMESTAMP '2024-02-29 13:45:30.123456',"
                    + " TIMESTAMPTZ '2024-02-29 13:45:30.5+00'),"
                    + "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
                    + ") t(id, b, ti, si, bi, f, d, dec, s, bin, dt, tm, ts, tstz)) TO '"
                    + types.resolve("part-0.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT range::BIGINT AS n, (range % 7)::INTEGER AS m FROM range(" + NUMBERS
                    + ")) TO '" + numbers.resolve("part-0.parquet") + "' (FORMAT parquet)");
        }
    }

    @Override
    public void close() throws Exception {
        helper.close();
        try (var paths = Files.walk(dataDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
    }
}
