package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Lookup joins in the MiniCluster with arrow-flight tables as the dimension side: every key type, composite
 * keys, several rows per key, NULL keys, quoted strings, dimension conditions, the partial cache, and
 * partial mode on a two-node cluster (each lookup reads the endpoints of both nodes).
 */
@Tag("integration")
class FlightLookupIntegrationTest {

    private static TestServer server;
    private static TestCluster cluster;
    private static TableEnvironment env;

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start();
        cluster = TestCluster.start(2);
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("table.local-time-zone", "UTC");
        env.getConfig().set("parallelism.default", "2");
        String types = "id INT, b BOOLEAN, ti TINYINT, si SMALLINT, bi BIGINT, f FLOAT, d DOUBLE,"
                + " `dec` DECIMAL(10, 2), s STRING, bin BYTES, dt DATE, tm TIME(3), ts TIMESTAMP(6),"
                + " tstz TIMESTAMP_LTZ(6)";
        env.executeSql("CREATE TABLE types (" + types + ") WITH (" + with(server.options("sparrow.types")) + ")");
        env.executeSql("CREATE TABLE types_probe (" + types + ", proc AS PROCTIME()) WITH ("
                + with(server.options("sparrow.types")) + ")");
        env.executeSql("CREATE TABLE dims (id BIGINT, name STRING, ts TIMESTAMP(6), v INT) WITH ("
                + with(server.options("sparrow.dims")) + ")");
        env.executeSql("CREATE TABLE dims_probe (id BIGINT, name STRING, ts TIMESTAMP(6), v INT,"
                + " proc AS PROCTIME()) WITH (" + with(server.options("sparrow.dims")) + ")");
        env.executeSql("CREATE TABLE keys (n BIGINT, proc AS PROCTIME()) WITH ("
                + with(server.options("SELECT n FROM sparrow.numbers WHERE n < 6")) + ")");
        env.executeSql("CREATE TABLE cluster_probe (n BIGINT, proc AS PROCTIME()) WITH ("
                + with(cluster.options("SELECT n FROM sparrow.numbers WHERE n % 997 = 0")) + ")");
        env.executeSql("CREATE TABLE cluster_numbers (n BIGINT, m INT) WITH ("
                + with(cluster.options("sparrow.numbers")) + ")");
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

    private static String with(Map<String, String> options) {
        return options.entrySet().stream().map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    private static List<Row> query(String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(sql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void joinsOnEveryKeyType() throws Exception {
        // The all-null row 2 matches nothing (NULL keys never match), row 1 matches itself on all keys.
        assertEquals(List.of(Row.of(1, 1, "héllo")), query("SELECT p.id, t.id, t.s FROM types_probe AS p"
                + " JOIN types FOR SYSTEM_TIME AS OF p.proc AS t ON t.b = p.b AND t.ti = p.ti AND t.si = p.si"
                + " AND t.bi = p.bi AND t.`dec` = p.`dec` AND t.s = p.s AND t.dt = p.dt AND t.ts = p.ts"
                + " AND t.id = p.id"));
        assertEquals(List.of(Row.of(1, 1), Row.of(2, null)), query("SELECT p.id, t.id FROM types_probe AS p"
                + " LEFT JOIN types FOR SYSTEM_TIME AS OF p.proc AS t ON t.ts = p.ts ORDER BY p.id"));
    }

    @Test
    void returnsEveryRowOfAKeyAndNothingForMissingKeys() throws Exception {
        assertEquals(List.of(Row.of(0L, null), Row.of(1L, "it's"), Row.of(2L, "b"), Row.of(2L, "b2"),
                Row.of(3L, null), Row.of(4L, null), Row.of(5L, null)),
                query("SELECT k.n, d.name FROM keys AS k LEFT JOIN dims FOR SYSTEM_TIME AS OF k.proc AS d"
                        + " ON d.id = k.n ORDER BY k.n, d.name"));
    }

    @Test
    void quotedStringsAndWholeMinuteTimestampsAreExactKeys() throws Exception {
        assertEquals(List.of(Row.of(1L, 10), Row.of(2L, 20), Row.of(2L, 21)),
                query("SELECT p.id, d.v FROM dims_probe AS p JOIN dims FOR SYSTEM_TIME AS OF p.proc AS d"
                        + " ON d.name = p.name AND d.ts = p.ts ORDER BY p.id, d.v"));
    }

    @Test
    void dimensionFiltersAndTheCacheKeepTheAnswers() throws Exception {
        String join = "SELECT k.n, d.v FROM keys AS k JOIN dims %s FOR SYSTEM_TIME AS OF k.proc AS d"
                + " ON d.id = k.n AND d.v > 15 ORDER BY k.n, d.v";
        List<Row> expected = List.of(Row.of(2L, 20), Row.of(2L, 21), Row.of(3L, 30));
        assertEquals(expected, query(String.format(join, "")));
        // Flink evaluates the dimension condition on the looked-up rows (where=[...]); the answers do not change.
        assertTrue(env.explainSql(String.format(join, "")).contains(
                "LookupJoin(table=[default_catalog.default_database.dims], joinType=[InnerJoin], lookup=[id=n]"));
        assertEquals(expected, query(String.format(join, "/*+ OPTIONS('lookup.cache' = 'PARTIAL',"
                + " 'lookup.partial-cache.max-rows' = '100', 'lookup.partial-cache.cache-missing-key' = 'true') */")));
    }

    @Test
    void rejectsKeysWithoutExactEquality() {
        Exception e = assertThrows(Exception.class, () -> query("SELECT p.id FROM types_probe AS p"
                + " JOIN types FOR SYSTEM_TIME AS OF p.proc AS t ON t.d = p.d"));
        assertTrue(messages(e).contains("cannot be a lookup key"), messages(e));
    }

    @Test
    void partialModeLooksUpOnEveryNode() throws Exception {
        // Keys spread over all files (both nodes); every probe row finds its one row.
        List<Row> rows = query("SELECT count(*), sum(c.m), sum(p.n) FROM cluster_probe AS p"
                + " JOIN cluster_numbers FOR SYSTEM_TIME AS OF p.proc AS c ON c.n = p.n");
        long count = 0;
        long sumM = 0;
        long sumN = 0;
        for (long n = 0; n < TestCluster.NUMBERS; n += 997) {
            count++;
            sumM += n % 7;
            sumN += n;
        }
        assertEquals(List.of(Row.of(count, (int) sumM, sumN)), rows);
    }

    @Test
    void looksUpInStreamingMode() throws Exception {
        TableEnvironment streaming = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        streaming.getConfig().set("parallelism.default", "2");
        streaming.executeSql("CREATE TABLE keys (n BIGINT, proc AS PROCTIME()) WITH ("
                + with(server.options("SELECT n FROM sparrow.numbers WHERE n < 6")) + ")");
        streaming.executeSql("CREATE TABLE dims (id BIGINT, name STRING, ts TIMESTAMP(6), v INT) WITH ("
                + with(server.options("sparrow.dims")) + ")");
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = streaming.executeSql("SELECT k.n, d.v FROM keys AS k"
                + " JOIN dims FOR SYSTEM_TIME AS OF k.proc AS d ON d.id = k.n").collect()) {
            it.forEachRemaining(r -> rows.add(Row.of(r.getField(0), r.getField(1))));
        }
        rows.sort(java.util.Comparator.comparing((Row r) -> (Long) r.getField(0))
                .thenComparing(r -> (Integer) r.getField(1)));
        assertEquals(List.of(Row.of(1L, 10), Row.of(2L, 20), Row.of(2L, 21), Row.of(3L, 30)), rows);
    }

    private static String messages(Throwable t) {
        StringBuilder b = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            b.append(c.getMessage()).append('\n');
        }
        return b.toString();
    }
}
