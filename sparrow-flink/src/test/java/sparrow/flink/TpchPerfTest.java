package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Wall time of the 22 TPC-H queries through Flink SQL, the native connector against Flink's JDBC connector with
 * the Arrow Flight SQL JDBC driver ({@link FlightSqlJdbcFactory}; parameters inlined by
 * {@link InliningFlightSqlDriver}, the server takes none), on the same two-node in-process cluster and
 * Flink settings (batch mode, parallelism 2). Three sources per query:
 * <ul>
 *   <li>{@code native}: {@code arrow-flight} catalog tables (partial mode: one split per endpoint, both nodes);</li>
 *   <li>{@code jdbc}: JDBC tables, one split per table (the driver reads the server's endpoints itself);</li>
 *   <li>{@code jdbc-part}: JDBC tables with the large tables split into {@code parallelism} key ranges
 *       ({@code scan.partition.*}), the best a JDBC user can do.</li>
 * </ul>
 * A first row {@code scan} reads all of lineitem (every column folded by an aggregate, no filter), the cost of
 * moving and converting the rows alone. Every answer is also compared with DuckDB's on the same files (as in {@link TpchIntegrationTest}), so a run at
 * SF 1 also checks the SF 1 answers. Each query runs once per source after one untimed warm-up run of Q1 per
 * source; times include planning and job start. Local runs only (shared CI runners give no stable numbers):
 * {@code ./mvnw -Pperf test -pl sparrow-flink -Dtest=TpchPerfTest [-Dtpch.sf=1] [-Dtpch.queries=scan,1,6]}; the table
 * is printed and written to {@code target/tpch-perf.txt}.
 */
@Tag("perf")
class TpchPerfTest {

    private static final double SCALE = Double.parseDouble(System.getProperty("tpch.sf", "1"));
    private static final int PARALLELISM = 2;
    private static final List<String> SOURCES = List.of("native", "jdbc", "jdbc-part");

    /**
     * All of lineitem read and folded in Flink: the transport and conversion cost without the query work (INTEGER
     * sums as BIGINT: Flink's SUM keeps the argument type and would overflow).
     */
    private static final String SCAN = "SELECT count(*), sum(l_orderkey), sum(CAST(l_partkey AS BIGINT)),"
            + " sum(CAST(l_suppkey AS BIGINT)), sum(CAST(l_linenumber AS BIGINT)), sum(l_quantity), sum(l_extendedprice), sum(l_discount), sum(l_tax),"
            + " max(l_returnflag), max(l_linestatus), max(l_shipdate), max(l_commitdate), max(l_receiptdate),"
            + " max(l_shipinstruct), max(l_shipmode), max(l_comment) FROM lineitem";

    /** Large tables and the integer key their JDBC scan is split by. */
    private static final Map<String, String> PARTITION_KEYS = Map.of("part", "p_partkey", "partsupp", "ps_partkey",
            "customer", "c_custkey", "orders", "o_orderkey", "lineitem", "l_orderkey");

    private static TestCluster cluster;
    private static TableEnvironment env;
    private static Connection duckdb;

    @BeforeAll
    static void start() throws Exception {
        cluster = TestCluster.start(2, null, dir -> TpchData.write(dir, SCALE));
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("table.local-time-zone", "UTC");
        env.getConfig().set("parallelism.default", String.valueOf(PARALLELISM));
        Map<String, String> options = new HashMap<>(cluster.catalogOptions());
        options.put("default-database", "tpch");
        env.executeSql("CREATE CATALOG fl WITH (" + with(options) + ")");
        duckdb = DriverManager.getConnection("jdbc:duckdb:");
        try (Statement s = duckdb.createStatement()) {
            for (String table : TpchData.TABLES) {
                s.execute("CREATE VIEW " + table + " AS SELECT * FROM read_parquet('"
                        + cluster.dataDir().resolve("tpch").resolve(table) + "/*.parquet')");
            }
        }
        String url = "jdbc:arrow-flight-sql://localhost:" + cluster.node(0).port() + "/?useEncryption=false";
        env.executeSql("CREATE DATABASE default_catalog.jdbc");
        env.executeSql("CREATE DATABASE default_catalog.`jdbc-part`");
        for (String table : TpchData.TABLES) {
            Map<String, String> jdbc = new LinkedHashMap<>(Map.of("connector", "jdbc", "url", url,
                    "driver", InliningFlightSqlDriver.class.getName(), "table-name", "tpch." + table, "scan.fetch-size",
                    "10000"));
            env.executeSql("CREATE TABLE default_catalog.jdbc." + table + " WITH (" + with(jdbc)
                    + ") LIKE fl.tpch." + table + " (EXCLUDING OPTIONS)");
            String key = PARTITION_KEYS.get(table);
            if (key != null) {
                long[] range = keyRange(table, key);
                jdbc.put("scan.partition.column", key);
                jdbc.put("scan.partition.num", String.valueOf(PARALLELISM));
                jdbc.put("scan.partition.lower-bound", String.valueOf(range[0]));
                jdbc.put("scan.partition.upper-bound", String.valueOf(range[1]));
            }
            env.executeSql("CREATE TABLE default_catalog.`jdbc-part`." + table + " WITH (" + with(jdbc)
                    + ") LIKE fl.tpch." + table + " (EXCLUDING OPTIONS)");
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
    void wallTimes() throws Exception {
        List<String> queries = queries();
        for (String source : SOURCES) {
            use(source);
            TpchIntegrationTest.flink(env, TpchIntegrationTest.read("tpch/q1.sql"));
        }
        StringBuilder report = new StringBuilder(String.format(Locale.ROOT,
                "TPC-H SF %s, 2 nodes, Flink parallelism %d, wall time in s%n%-5s %9s %9s %9s %8s %8s%n", SCALE,
                PARALLELISM, "query", SOURCES.get(0), SOURCES.get(1), SOURCES.get(2), "jdbc/nat", "part/nat"));
        System.out.print(report);
        double[] totals = new double[SOURCES.size()];
        List<String> wrong = new ArrayList<>();
        for (String q : queries) {
            String sql = "scan".equals(q) ? SCAN : TpchIntegrationTest.read("tpch/q" + q.substring(1) + ".sql");
            List<List<Object>> expected = TpchIntegrationTest.duckDb(duckdb, sql);
            double[] seconds = new double[SOURCES.size()];
            for (int i = 0; i < SOURCES.size(); i++) {
                use(SOURCES.get(i));
                long t0 = System.nanoTime();
                List<List<Object>> actual = TpchIntegrationTest.flink(env, sql);
                seconds[i] = (System.nanoTime() - t0) / 1e9;
                totals[i] += seconds[i];
                String difference = difference(expected, actual);
                if (difference != null) {
                    wrong.add(q + " via " + SOURCES.get(i) + ": " + difference);
                }
            }
            String row = line(q, seconds);
            report.append(row);
            System.out.print(row);
        }
        report.append(line("total", totals));
        System.out.print(line("total", totals));
        Files.writeString(Path.of("target", "tpch-perf.txt"), report, StandardCharsets.UTF_8);
        assertTrue(wrong.isEmpty(), "answers differ from DuckDB's: " + wrong);
    }

    private static String line(String label, double[] seconds) {
        return String.format(Locale.ROOT, "%-5s %9.2f %9.2f %9.2f %8.2f %8.2f%n", label, seconds[0], seconds[1],
                seconds[2], seconds[1] / seconds[0], seconds[2] / seconds[0]);
    }

    /** The first difference of two answers, or null if they are the same. */
    private static String difference(List<List<Object>> expected, List<List<Object>> actual) {
        if (expected.size() != actual.size()) {
            return expected.size() + " rows expected, " + actual.size() + " returned";
        }
        for (int i = 0; i < expected.size(); i++) {
            if (!TpchIntegrationTest.same(expected.get(i), actual.get(i))) {
                return "row " + i + " expected " + expected.get(i) + " but was " + actual.get(i);
            }
        }
        return null;
    }

    /** {@code scan} and {@code Q1}..{@code Q22}, or those listed in {@code -Dtpch.queries} (numbers or scan). */
    private static List<String> queries() {
        String list = System.getProperty("tpch.queries", "");
        if (list.isBlank()) {
            return Stream.concat(Stream.of("scan"), IntStream.rangeClosed(1, 22).mapToObj(q -> "Q" + q)).toList();
        }
        return Arrays.stream(list.split(",")).map(String::trim).map(q -> "scan".equals(q) ? q : "Q" + q).toList();
    }

    private static void use(String source) {
        if ("native".equals(source)) {
            env.useCatalog("fl");
            env.useDatabase("tpch");
        } else {
            env.useCatalog("default_catalog");
            env.useDatabase(source);
        }
    }

    private static long[] keyRange(String table, String key) throws Exception {
        try (Statement s = duckdb.createStatement();
                ResultSet r = s.executeQuery("SELECT min(" + key + "), max(" + key + ") FROM " + table)) {
            assertTrue(r.next());
            long[] range = {r.getLong(1), r.getLong(2)};
            assertEquals(false, r.next());
            return range;
        }
    }

    private static String with(Map<String, String> options) {
        return options.entrySet().stream().map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    }
}
