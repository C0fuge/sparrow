package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
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

/** Flink SQL in local (MiniCluster) batch mode reads connector tables from the in-process server. */
@Tag("integration")
class FlightSourceIntegrationTest {

    private static TestServer server;
    private static TableEnvironment env;

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start();
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("table.local-time-zone", "UTC");
        env.getConfig().set("parallelism.default", "2");
        env.executeSql("CREATE TABLE types (id INT, b BOOLEAN, ti TINYINT, si SMALLINT, bi BIGINT, f FLOAT,"
                + " d DOUBLE, `dec` DECIMAL(10, 2), s STRING, bin BYTES, dt DATE, tm TIME(3), ts TIMESTAMP(6),"
                + " tstz TIMESTAMP_LTZ(6)) WITH (" + with(server.options("sparrow.types")) + ")");
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH ("
                + with(server.options("sparrow.numbers")) + ")");
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
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
    void readsEverySupportedType() throws Exception {
        List<Row> rows = query("SELECT * FROM types ORDER BY id");
        assertEquals(2, rows.size());
        Row r = rows.get(0);
        assertEquals(1, r.getField("id"));
        assertEquals(true, r.getField("b"));
        assertEquals((byte) 7, r.getField("ti"));
        assertEquals((short) 300, r.getField("si"));
        assertEquals(9_000_000_000L, r.getField("bi"));
        assertEquals(1.5f, r.getField("f"));
        assertEquals(2.25d, r.getField("d"));
        assertEquals(new BigDecimal("12345.67"), r.getField("dec"));
        assertEquals("héllo", r.getField("s"));
        assertTrue(Arrays.equals(new byte[] {(byte) 0xCA, (byte) 0xFE}, (byte[]) r.getField("bin")));
        assertEquals(LocalDate.of(2024, 2, 29), r.getField("dt"));
        assertEquals(LocalTime.of(13, 45, 30, 123_000_000), r.getField("tm"));
        assertEquals(LocalDateTime.of(2024, 2, 29, 13, 45, 30, 123_456_000), r.getField("ts"));
        assertEquals(Instant.parse("2024-02-29T13:45:30.5Z"), r.getField("tstz"));
        Row nulls = rows.get(1);
        assertEquals(2, nulls.getField("id"));
        for (int i = 1; i < nulls.getArity(); i++) {
            assertEquals(null, nulls.getField(i), "column " + i);
        }
    }

    @Test
    void readsAllRowsOfAMultiBatchTable() throws Exception {
        List<Row> rows = query("SELECT count(*), sum(n), count(DISTINCT n) FROM numbers");
        long n = TestServer.NUMBERS;
        assertEquals(Row.of(n, n * (n - 1) / 2, n), rows.get(0));
    }

    @Test
    void projectsColumns() throws Exception {
        List<Row> rows = query("SELECT m, count(*) AS c FROM numbers GROUP BY m ORDER BY m");
        assertEquals(7, rows.size());
        long total = rows.stream().mapToLong(r -> (Long) r.getField("c")).sum();
        assertEquals(TestServer.NUMBERS, total);
    }

    @Test
    void pushedFiltersGiveTheSameAnswers() throws Exception {
        long n = TestServer.NUMBERS;
        // n in [0, NUMBERS), m = n % 7: the expected answers are computed here.
        assertEquals(List.of(Row.of(100L, 1L)), query("SELECT count(*), min(n) FROM numbers"
                + " WHERE n >= 1 AND n <= 100"));
        assertEquals(List.of(Row.of(n / 7 + (n % 7 > 3 ? 1 : 0))),
                query("SELECT count(*) FROM numbers WHERE m = 3"));
        assertEquals(List.of(Row.of(5L)), query("SELECT count(*) FROM numbers WHERE n IN (1, 2, 3, 70000, -5)"
                + " OR (n > 99990 AND m = 0)"));
        assertEquals(List.of(Row.of(2L)), query("SELECT count(*) FROM numbers WHERE 99998 <= n"));
        // a filter the source cannot take (expression on a column) stays in Flink next to pushed ones
        assertEquals(List.of(Row.of(30L)), query("SELECT count(*) FROM numbers WHERE n < 100 AND MOD(n, 7) = m"
                + " AND m < 2"));
        assertTrue(explain("SELECT n FROM numbers WHERE n > 5 AND m <> 2").contains("filter=["));
    }

    @Test
    void pushedFiltersOnEveryTranslatedTypeAndNulls() throws Exception {
        assertEquals(List.of(Row.of(1)), query("SELECT id FROM types WHERE b AND ti = 7 AND si < 301 AND bi > 1"
                + " AND `dec` = 12345.67 AND s LIKE 'h%' AND s LIKE '%llo' AND s LIKE '%éll%' AND s = 'héllo'"
                + " AND dt = DATE '2024-02-29' AND ts > TIMESTAMP '2024-02-29 13:45:30.123455'"));
        // three-valued logic: the all-null row matches neither a predicate nor its negation
        assertEquals(List.of(), query("SELECT id FROM types WHERE NOT (ti = 7)"));
        assertEquals(List.of(Row.of(2)), query("SELECT id FROM types WHERE ti IS NULL AND s IS NULL"));
        assertEquals(List.of(Row.of(1)), query("SELECT id FROM types WHERE NOT b IS NULL"));
        // double/float filters stay in Flink and still filter
        assertEquals(List.of(Row.of(1)), query("SELECT id FROM types WHERE d > 2.0 AND f < 2.0"));
    }

    @Test
    void pushedLimitKeepsTheRequestedRowCount() throws Exception {
        assertEquals(5, query("SELECT n FROM numbers LIMIT 5").size());
        assertEquals(3, query("SELECT n FROM numbers WHERE m = 1 LIMIT 3").stream()
                .filter(r -> (Long) r.getField(0) % 7 == 1).count());
        assertTrue(explain("SELECT n FROM numbers LIMIT 5").contains("limit=[5]"));
    }

    @Test
    void failsInsteadOfNarrowingAValueThatDoesNotFit() {
        // bi is BIGINT on the server and holds 9 000 000 000: declared as INT the read must fail, not wrap.
        env.executeSql("CREATE TABLE narrow (bi INT) WITH (" + with(server.options("SELECT bi FROM sparrow.types"))
                + ")");
        Exception e = assertThrows(Exception.class, () -> query("SELECT * FROM narrow"));
        assertTrue(rootMessage(e).contains("out of range"), rootMessage(e));
    }

    private static String explain(String sql) {
        return env.explainSql(sql);
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getMessage();
    }
}
