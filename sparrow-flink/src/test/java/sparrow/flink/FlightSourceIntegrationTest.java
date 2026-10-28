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
    void failsInsteadOfNarrowingAValueThatDoesNotFit() {
        // bi is BIGINT on the server and holds 9 000 000 000: declared as INT the read must fail, not wrap.
        env.executeSql("CREATE TABLE narrow (bi INT) WITH (" + with(server.options("SELECT bi FROM sparrow.types"))
                + ")");
        Exception e = assertThrows(Exception.class, () -> query("SELECT * FROM narrow"));
        assertTrue(rootMessage(e).contains("out of range"), rootMessage(e));
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getMessage();
    }
}
