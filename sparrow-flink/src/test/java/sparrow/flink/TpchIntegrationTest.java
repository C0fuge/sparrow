package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;

/**
 * The 22 TPC-H queries (SF 0.1, {@link TpchData}) through Flink SQL over {@code arrow-flight} catalog tables
 * of a two-node cluster in partial mode, each answer compared with DuckDB's on the same Parquet files: same
 * rows in the same order, numbers equal up to a relative 1e-9; where DuckDB computes an average or
 * quotient in DOUBLE and Flink in DECIMAL, the double rounded half-up to Flink's scale must equal Flink's
 * value. Calcite's assertions are disabled in the tests (root pom): with {@code -ea} its Volcano planner
 * fails Q18 and Q21 with an internal "has wrong best cost" check that production (no {@code -ea}) skips. The query texts ({@code tpch/q<n>.sql}) are the TPC-H ones with the
 * validation parameters, written in the SQL both engines accept: {@code WITH} instead of the view in Q15,
 * {@code val} for the reserved alias {@code value} in Q11, the sub-query column alias of Q13 moved inside,
 * and key columns added to the {@code ORDER BY} of Q3, Q10, Q11 and Q18 so that ties have one order.
 */
@Tag("integration")
class TpchIntegrationTest {

    private static final double SCALE = 0.1;
    private static final BigDecimal RELATIVE = new BigDecimal("1e-9");

    private static TestCluster cluster;
    private static TableEnvironment env;
    private static Connection duckdb;

    @BeforeAll
    static void start() throws Exception {
        cluster = TestCluster.start(2, null, dir -> TpchData.write(dir, SCALE));
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("table.local-time-zone", "UTC");
        env.getConfig().set("parallelism.default", "2");
        Map<String, String> options = new HashMap<>(cluster.catalogOptions());
        options.put("default-database", "tpch");
        env.executeSql("CREATE CATALOG fl WITH (" + options.entrySet().stream()
                .map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'").collect(Collectors.joining(", ")) + ")");
        env.executeSql("USE CATALOG fl");
        duckdb = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = duckdb.createStatement()) {
            for (String table : TpchData.TABLES) {
                s.execute("CREATE VIEW " + table + " AS SELECT * FROM read_parquet('"
                        + cluster.dataDir().resolve("tpch").resolve(table) + "/*.parquet')");
            }
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (duckdb != null) {
            duckdb.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    @Test
    void largeTablesArePlannedOnBothNodes() {
        Configuration config = FlightOptions.toClientConfiguration(
                org.apache.flink.configuration.Configuration.fromMap(cluster.options("tpch.lineitem")));
        Set<Integer> primaries = new HashSet<>();
        for (Endpoint e : Client.getOrCreate(config).getQueryEndpoints(
                "SELECT * FROM " + FlightCatalog.serverName("tpch", "lineitem")).getEndpoints()) {
            primaries.add(e.getURIs()[0].getPort());
        }
        assertEquals(2, primaries.size(), "lineitem endpoints on both nodes");
    }

    static IntStream queries() {
        return IntStream.rangeClosed(1, 22);
    }

    @ParameterizedTest(name = "Q{0}")
    @MethodSource("queries")
    void answersEqualDuckDb(int query) throws Exception {
        String sql = read("tpch/q" + query + ".sql");
        List<List<Object>> expected = duckDb(duckdb, sql);
        List<List<Object>> actual = flink(env, sql);
        assertFalse(expected.isEmpty(), "the data must give the query an answer");
        assertEquals(expected.size(), actual.size(), "rows of Q" + query + ": " + actual);
        for (int i = 0; i < expected.size(); i++) {
            if (!same(expected.get(i), actual.get(i))) {
                fail("Q" + query + " row " + i + ": expected " + expected.get(i) + " but Flink returned "
                        + actual.get(i));
            }
        }
    }

    /** Whether two normalized rows are equal (numbers as in the class comment). */
    static boolean same(List<Object> expected, List<Object> actual) {
        if (expected.size() != actual.size()) {
            return false;
        }
        for (int i = 0; i < expected.size(); i++) {
            Object e = expected.get(i);
            Object a = actual.get(i);
            if (e instanceof Double x && a instanceof BigDecimal y) {
                // DuckDB's DOUBLE average/quotient vs Flink's DECIMAL one (rounded half-up to its scale)
                if (BigDecimal.valueOf(x).setScale(y.scale(), RoundingMode.HALF_UP).compareTo(y) != 0) {
                    return false;
                }
            } else if (e instanceof BigDecimal x && a instanceof BigDecimal y) {
                BigDecimal tolerance = x.abs().max(y.abs()).multiply(RELATIVE, MathContext.DECIMAL64);
                if (x.subtract(y).abs().compareTo(tolerance) > 0) {
                    return false;
                }
            } else if (!String.valueOf(e).equals(String.valueOf(a))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Decimals and doubles as they are, other numbers as BigDecimal, everything else as its string (dates
     * print as yyyy-MM-dd in both).
     */
    static Object normalize(Object value) {
        if (value instanceof BigDecimal || value instanceof Double) {
            return value;
        }
        if (value instanceof Float f) {
            return f.doubleValue();
        }
        if (value instanceof Number n) {
            return BigDecimal.valueOf(n.longValue());
        }
        return value == null ? null : value.toString();
    }

    /** DuckDB's normalized answer to {@code sql}. */
    static List<List<Object>> duckDb(Connection connection, String sql) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        try (Statement s = connection.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int columns = r.getMetaData().getColumnCount();
            while (r.next()) {
                List<Object> row = new ArrayList<>();
                for (int c = 1; c <= columns; c++) {
                    row.add(normalize(r.getObject(c)));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** Flink's normalized answer to {@code sql}. */
    static List<List<Object>> flink(TableEnvironment tableEnv, String sql) throws Exception {
        List<List<Object>> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = tableEnv.executeSql(sql).collect()) {
            while (it.hasNext()) {
                Row r = it.next();
                List<Object> row = new ArrayList<>();
                for (int c = 0; c < r.getArity(); c++) {
                    row.add(normalize(r.getField(c)));
                }
                rows.add(row);
            }
        }
        return rows;
    }

    /** A UTF-8 test resource. */
    static String read(String resource) throws IOException {
        try (InputStream in = TpchIntegrationTest.class.getClassLoader().getResourceAsStream(resource)) {
            if (in == null) {
                throw new IOException("missing resource " + resource);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
