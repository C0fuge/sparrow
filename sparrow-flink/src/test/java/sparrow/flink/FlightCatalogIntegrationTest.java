package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** An {@code arrow-flight} catalog in Flink SQL (local MiniCluster, batch mode) over the in-process server. */
@Tag("integration")
class FlightCatalogIntegrationTest {

    private static TestServer server;
    private static TableEnvironment env;

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start();
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("table.local-time-zone", "UTC");
        env.getConfig().set("parallelism.default", "2");
        env.executeSql("CREATE CATALOG fl WITH (" + with(server.catalogOptions()) + ")");
        // the same table declared by DDL, to compare answers
        env.executeSql("CREATE TABLE types (id INT, b BOOLEAN, ti TINYINT, si SMALLINT, bi BIGINT, f FLOAT,"
                + " d DOUBLE, `dec` DECIMAL(10, 2), s STRING, bin BYTES, dt DATE, tm TIME(3), ts TIMESTAMP(6),"
                + " tstz TIMESTAMP_LTZ(6)) WITH (" + with(server.options("sparrow.types")) + ")");
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

    private static List<String> firstColumn(String sql) throws Exception {
        return query(sql).stream().map(r -> String.valueOf(r.getField(0))).sorted().toList();
    }

    private static Catalog catalog() {
        return env.getCatalog("fl").orElseThrow();
    }

    @Test
    void listsSchemasAsDatabasesAndTheirTables() throws Exception {
        assertTrue(firstColumn("SHOW DATABASES IN fl").contains("sparrow"));
        assertEquals(List.of("dims", "nested", "numbers", "types"), firstColumn("SHOW TABLES IN fl.sparrow"));
        assertTrue(catalog().tableExists(new ObjectPath("sparrow", "numbers")));
        assertFalse(catalog().tableExists(new ObjectPath("sparrow", "numbers_")));
        assertFalse(catalog().databaseExists("no_such_schema"));
    }

    @Test
    void mapsColumnTypesFromTheServerSchema() throws Exception {
        CatalogBaseTable table = catalog().getTable(new ObjectPath("sparrow", "types"));
        Schema expected = Schema.newBuilder().column("id", DataTypes.INT()).column("b", DataTypes.BOOLEAN())
                .column("ti", DataTypes.TINYINT()).column("si", DataTypes.SMALLINT())
                .column("bi", DataTypes.BIGINT()).column("f", DataTypes.FLOAT()).column("d", DataTypes.DOUBLE())
                .column("dec", DataTypes.DECIMAL(10, 2)).column("s", DataTypes.STRING())
                .column("bin", DataTypes.BYTES()).column("dt", DataTypes.DATE()).column("tm", DataTypes.TIME(3))
                .column("ts", DataTypes.TIMESTAMP(6)).column("tstz", DataTypes.TIMESTAMP_LTZ(6)).build();
        assertEquals(expected.toString(), table.getUnresolvedSchema().toString());
        // the connection (and its token) stays in the catalog, not in the table options
        assertEquals(Map.of("connector", "arrow-flight", "table", "sparrow.types"), table.getOptions());
    }

    @Test
    void readsTheSameAnswersAsADdlTable() throws Exception {
        assertEquals(query("SELECT * FROM types ORDER BY id").toString(),
                query("SELECT * FROM fl.sparrow.types ORDER BY id").toString());
        long n = TestServer.NUMBERS;
        assertEquals(List.of(Row.of(n, n * (n - 1) / 2)), query("SELECT count(*), sum(n) FROM fl.sparrow.numbers"));
        assertEquals(List.of(Row.of(100L)), query("SELECT count(*) FROM fl.sparrow.numbers WHERE n < 100"));
        assertTrue(env.explainSql("SELECT n FROM fl.sparrow.numbers WHERE n > 5 LIMIT 3").contains("filter=["));
    }

    @Test
    void defaultDatabaseResolvesUnqualifiedNames() throws Exception {
        env.executeSql("USE CATALOG fl");
        try {
            assertEquals(List.of(Row.of((long) TestServer.NUMBERS)), query("SELECT count(*) FROM numbers"));
        } finally {
            env.executeSql("USE CATALOG default_catalog");
        }
    }

    @Test
    void leavesOutColumnsTheConnectorCannotRead() throws Exception {
        assertEquals(List.of("id", "s"), catalog().getTable(new ObjectPath("sparrow", "nested"))
                .getUnresolvedSchema().getColumns().stream().map(Schema.UnresolvedColumn::getName).toList());
        assertEquals(List.of(Row.of(1, "x")), query("SELECT * FROM fl.sparrow.nested"));
    }

    @Test
    void unknownTablesAndChangesAreRejected() {
        assertThrows(TableNotExistException.class, () -> catalog().getTable(new ObjectPath("sparrow", "nope")));
        Exception e = assertThrows(Exception.class, () -> env.executeSql("DROP TABLE fl.sparrow.numbers"));
        assertTrue(rootMessage(e).contains("read-only"), rootMessage(e));
        assertThrows(Exception.class, () -> env.executeSql("CREATE CATALOG bad WITH ('type' = 'arrow-flight',"
                + " 'host' = 'localhost', 'port' = '" + server.port + "', 'default-database' = 'no_such_schema')"));
    }

    private static String rootMessage(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) {
            c = c.getCause();
        }
        return c.getMessage();
    }
}
