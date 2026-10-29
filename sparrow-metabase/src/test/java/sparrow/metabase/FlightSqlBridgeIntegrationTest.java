package sparrow.metabase;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The bridge against an in-process server: metadata as Metabase's sync reads it, values of every mapped type,
 * multi-batch results, aggregation, cancellation and errors.
 */
@Tag("integration")
class FlightSqlBridgeIntegrationTest {

    private static TestServer server;
    private static FlightSqlBridge bridge;

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start();
        bridge = FlightSqlBridge.connect(server.details());
    }

    @AfterAll
    static void stop() throws Exception {
        server.close();
    }

    private static List<Object[]> rows(String sql) {
        List<Object[]> rows = new ArrayList<>();
        try (QueryResult result = bridge.execute(sql)) {
            result.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void ping() {
        assertTrue(bridge.ping());
    }

    @Test
    void metadata() {
        assertTrue(bridge.schemas().contains("sparrow"), bridge.schemas().toString());
        List<String> tables = bridge.tables("sparrow").stream().map(TableRef::name).sorted().toList();
        assertEquals(List.of("nested", "numbers", "types"), tables);
        assertTrue(bridge.tables(null).stream().anyMatch(t -> "sparrow".equals(t.schema())
                && "numbers".equals(t.name())));
        List<Column> columns = bridge.columns("sparrow", "types");
        assertEquals(List.of("id", "b", "ti", "si", "bi", "ut", "ub", "f", "d", "dec", "s", "bin", "dt", "tm",
                "ts", "tstz"), columns.stream().map(Column::name).toList());
        assertEquals(List.of("INTEGER", "BOOLEAN", "TINYINT", "SMALLINT", "BIGINT",
                // Parquet keeps unsigned integers as wider signed types
                "SMALLINT", "DECIMAL(20,0)", "FLOAT", "DOUBLE", "DECIMAL(10,2)", "VARCHAR", "BLOB", "DATE", "TIME", "TIMESTAMP",
                "TIMESTAMP WITH TIME ZONE"), columns.stream().map(Column::databaseType).toList());
        assertEquals(15, columns.get(15).position());
        assertEquals(List.of(), bridge.columns("sparrow", "missing"));
        // LIKE wildcards in names are not patterns here
        assertEquals(List.of(), bridge.columns("sparrow", "type_"));
    }

    @Test
    void valuesOfEveryType() {
        List<Object[]> rows = rows("SELECT * FROM sparrow.types ORDER BY id");
        assertEquals(2, rows.size());
        Object[] r = rows.get(0);
        assertEquals(1, r[0]);
        assertEquals(true, r[1]);
        assertEquals(-7, r[2]);
        assertEquals(300, r[3]);
        assertEquals(9_000_000_000L, r[4]);
        assertEquals(200, r[5]);
        assertEquals(new BigDecimal("18446744073709551615"), r[6]);
        assertEquals(1.5f, r[7]);
        assertEquals(2.25d, r[8]);
        assertEquals(new BigDecimal("12345.67"), r[9]);
        assertEquals("héllo", r[10]);
        assertArrayEquals(new byte[] {(byte) 0xCA, (byte) 0xFE}, (byte[]) r[11]);
        assertEquals(LocalDate.of(2024, 2, 29), r[12]);
        assertEquals(LocalTime.of(13, 45, 30, 123_456_000), r[13]);
        assertEquals(LocalDateTime.of(2024, 2, 29, 13, 45, 30, 123_456_000), r[14]);
        assertEquals(OffsetDateTime.of(2024, 2, 29, 13, 45, 30, 500_000_000, ZoneOffset.UTC),
                ((OffsetDateTime) r[15]).withOffsetSameInstant(ZoneOffset.UTC));
        Object[] nulls = rows.get(1);
        assertEquals(2, nulls[0]);
        for (int c = 1; c < nulls.length; c++) {
            assertNull(nulls[c], "column " + c);
        }
    }

    @Test
    void unsignedIntegers() {
        try (QueryResult result = bridge.execute("SELECT 200::UTINYINT AS a, 65535::USMALLINT AS b,"
                + " 4000000000::UINTEGER AS c, 18446744073709551615::UBIGINT AS d")) {
            assertEquals(List.of("UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT"),
                    result.columns().stream().map(Column::databaseType).toList());
            assertArrayEquals(new Object[] {200, 65535, 4_000_000_000L, new BigInteger("18446744073709551615")},
                    result.next());
            assertFalse(result.hasNext());
        }
    }

    @Test
    void resultColumns() {
        try (QueryResult result = bridge.execute("SELECT n, m * 2 AS twice, 'x' AS s FROM sparrow.numbers LIMIT 1")) {
            assertEquals(List.of("n", "twice", "s"), result.columns().stream().map(Column::name).toList());
            assertEquals(List.of("BIGINT", "INTEGER", "VARCHAR"),
                    result.columns().stream().map(Column::databaseType).toList());
        }
        assertEquals(List.of("BIGINT", "INTEGER"),
                bridge.describe("SELECT n, m FROM sparrow.numbers").stream().map(Column::databaseType).toList());
    }

    @Test
    void manyBatchesInOrder() {
        List<Object[]> rows = rows("SELECT n FROM sparrow.numbers ORDER BY n");
        assertEquals(TestServer.NUMBERS, rows.size());
        for (int i = 0; i < rows.size(); i++) {
            assertEquals((long) i, rows.get(i)[0]);
        }
    }

    @Test
    void aggregation() {
        List<Object[]> rows = rows("SELECT m, count(*) AS c, sum(n) AS s FROM sparrow.numbers GROUP BY m ORDER BY m");
        assertEquals(7, rows.size());
        for (int m = 0; m < 7; m++) {
            long count = 0;
            long sum = 0;
            for (long n = m; n < TestServer.NUMBERS; n += 7) {
                count++;
                sum += n;
            }
            assertEquals(m, rows.get(m)[0]);
            assertEquals(count, rows.get(m)[1]);
            assertEquals(new BigDecimal(sum), new BigDecimal(rows.get(m)[2].toString()));
        }
    }

    @Test
    void nestedValuesAsText() {
        List<Column> columns = bridge.columns("sparrow", "nested");
        assertEquals(List.of("INTEGER", "INTEGER[]", "STRUCT(\"a\" INTEGER, \"b\" VARCHAR)"),
                columns.stream().map(Column::databaseType).toList());
        Object[] row = rows("SELECT * FROM sparrow.nested").get(0);
        assertEquals("[1,2]", row[1]);
        assertEquals("{\"a\":1,\"b\":\"x\"}", row[2]);
    }

    @Test
    void earlyCloseAndCancel() {
        try (QueryResult result = bridge.execute("SELECT n FROM sparrow.numbers")) {
            assertTrue(result.hasNext());
            result.next();
        }
        QueryResult result = bridge.execute("SELECT n FROM sparrow.numbers");
        assertTrue(result.hasNext());
        result.cancel();
        assertThrows(CancellationException.class, () -> {
            while (result.hasNext()) {
                result.next();
            }
        });
        result.close();
        // the shared connection still works
        assertTrue(bridge.ping());
    }

    @Test
    void emptyResult() {
        try (QueryResult result = bridge.execute("SELECT n FROM sparrow.numbers WHERE n < 0")) {
            assertFalse(result.hasNext());
            assertEquals(1, result.columns().size());
        }
    }

    @Test
    void sqlErrorsReachTheCaller() {
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> rows("SELECT no_such_column FROM sparrow.numbers"));
        assertTrue(String.valueOf(e.getMessage()).contains("no_such_column"), e.toString());
    }

    @Test
    void unreachableServerFails() {
        Map<String, Object> details = server.details();
        details.put("port", 1);
        details.put("max-retries", 0);
        assertThrows(RuntimeException.class, () -> FlightSqlBridge.connect(details).ping());
    }
}
