package sparrow.metabase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import pro.surpin.data.arrowflight.server.TestFlightServerHelper;

/**
 * In-process Arrow Flight SQL server over Parquet data generated with DuckDB in a temporary directory:
 * {@code sparrow.types} (one row per DuckDB type the driver maps plus an all-null row), {@code sparrow.numbers}
 * (100 000 rows, several Arrow batches), {@code sparrow.nested} (list, struct and map columns) and the shop tables
 * of the Metabase smoke test, {@code shop.orders} ({@value #ORDERS} rows) and {@code shop.customers}
 * ({@value #CUSTOMERS} rows), whose values follow formulas that {@code driver/test/smoke.py} repeats to compute the
 * expected answers.
 */
final class TestServer implements AutoCloseable {

    /** Rows of {@code sparrow.numbers}. */
    static final int NUMBERS = 100_000;
    /** Rows of {@code shop.orders}. */
    static final int ORDERS = 1000;
    /** Rows of {@code shop.customers}. */
    static final int CUSTOMERS = 20;

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
        Path dir = Files.createTempDirectory("sparrow-metabase-test");
        writeTables(dir);
        return new TestServer(TestFlightServerHelper.builder().dataDir(dir.toString()).start(), dir);
    }

    /** Metabase connection details of this server (as the driver passes them: string keys). */
    Map<String, Object> details() {
        Map<String, Object> details = new HashMap<>();
        details.put("host", "localhost");
        details.put("port", port);
        details.put("user", "test-user");
        details.put("token", "sparrow-test");
        return details;
    }

    /** Writes the test tables as {@code <dir>/<schema>/<table>/*.parquet}. */
    private static void writeTables(Path dir) throws Exception {
        Path types = Files.createDirectories(dir.resolve("sparrow/types"));
        Path numbers = Files.createDirectories(dir.resolve("sparrow/numbers"));
        Path nested = Files.createDirectories(dir.resolve("sparrow/nested"));
        Path orders = Files.createDirectories(dir.resolve("shop/orders"));
        Path customers = Files.createDirectories(dir.resolve("shop/customers"));
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            s.execute("SET TimeZone = 'UTC'");
            s.execute("COPY (SELECT * FROM (VALUES "
                    + "(1, true, -7::TINYINT, 300::SMALLINT, 9000000000::BIGINT, 200::UTINYINT,"
                    + " 18446744073709551615::UBIGINT, 1.5::FLOAT, 2.25::DOUBLE, 12345.67::DECIMAL(10,2),"
                    + " 'héllo', '\\xCA\\xFE'::BLOB, DATE '2024-02-29', TIME '13:45:30.123456',"
                    + " TIMESTAMP '2024-02-29 13:45:30.123456', TIMESTAMPTZ '2024-02-29 13:45:30.5+00'),"
                    + "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
                    + ") t(id, b, ti, si, bi, ut, ub, f, d, dec, s, bin, dt, tm, ts, tstz)) TO '"
                    + types.resolve("part-0.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT range::BIGINT AS n, (range % 7)::INTEGER AS m FROM range(" + NUMBERS
                    + ")) TO '" + numbers.resolve("part-0.parquet") + "' (FORMAT parquet)");
            s.execute("COPY (SELECT 1 AS id, [1, 2] AS l, {'a': 1, 'b': 'x'} AS st) TO '"
                    + nested.resolve("part-0.parquet") + "' (FORMAT parquet)");
            // keep in sync with orders()/customers() in driver/test/smoke.py
            s.execute("COPY (SELECT i::INTEGER AS id, (i % " + CUSTOMERS + ")::INTEGER AS customer_id,"
                    + " DATE '2023-01-01' + ((i * 7) % 730)::INTEGER AS order_date,"
                    + " CAST(DATE '2023-01-01' + ((i * 7) % 730)::INTEGER AS TIMESTAMP)"
                    + " + to_seconds((i * 3637) % 86400) AS created_at,"
                    + " CAST(CAST(DATE '2023-01-01' + ((i * 7) % 730)::INTEGER AS TIMESTAMP)"
                    + " + to_seconds((i * 3637) % 86400) AS TIMESTAMPTZ) AS created_tz,"
                    + " CAST(((i * 37) % 10000) / 100 AS DECIMAL(10,2)) AS amount,"
                    + " ['open', 'shipped', 'returned'][i % 3 + 1] AS status,"
                    + " CASE i % 10 WHEN 0 THEN '50% off' WHEN 1 THEN 'gift_wrap' ELSE 'Note ' || i END AS note"
                    + " FROM range(" + ORDERS + ") t(i)) TO '" + orders.resolve("part-0.parquet")
                    + "' (FORMAT parquet)");
            s.execute("COPY (SELECT i::INTEGER AS id, 'Customer ' || i AS name,"
                    + " ['Berlin', 'Paris', 'Rome', 'Oslo'][i % 4 + 1] AS city FROM range(" + CUSTOMERS
                    + ") t(i)) TO '" + customers.resolve("part-0.parquet") + "' (FORMAT parquet)");
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
