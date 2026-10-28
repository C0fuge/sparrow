package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;

/**
 * Partial mode against a three-node cluster: one split per endpoint, and after the Flight server of a node is
 * killed (its cluster membership stays, so plans still send endpoints to it) the splits planned on it are read
 * from their backup locations with the same answers.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FlightFailoverIntegrationTest {

    private static final String QUERY = "SELECT n, m FROM sparrow.numbers";
    private static final long SUM = (long) TestCluster.NUMBERS * (TestCluster.NUMBERS - 1) / 2;

    private static TestCluster cluster;
    private static TableEnvironment env;
    private static Configuration config;

    @BeforeAll
    static void start() throws Exception {
        cluster = TestCluster.start(3);
        config = FlightOptions.toClientConfiguration(
                org.apache.flink.configuration.Configuration.fromMap(cluster.options("sparrow.numbers")));
        env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("parallelism.default", "2");
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH ("
                + with(cluster.options("sparrow.numbers")) + ")");
        env.executeSql("CREATE CATALOG fl WITH (" + with(cluster.catalogOptions()) + ")");
    }

    @AfterAll
    static void stop() throws Exception {
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

    private static Endpoint[] plan(String sql) {
        return Client.getOrCreate(config).getQueryEndpoints(sql).getEndpoints();
    }

    /** Count, distinct count, sum of {@code n} and sum of {@code m}: every row exactly once. */
    private static void assertComplete(String table) throws Exception {
        List<Row> rows = query("SELECT COUNT(*), COUNT(DISTINCT n), SUM(n), SUM(CAST(m AS BIGINT)) FROM " + table);
        long sumM = 0;
        for (int n = 0; n < TestCluster.NUMBERS; n++) {
            sumM += n % 7;
        }
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, (long) TestCluster.NUMBERS, SUM, sumM)), rows);
    }

    @Test
    @Order(1)
    void partialModePlansOneEndpointPerNodeWithBackups() throws Exception {
        Endpoint[] endpoints = plan(QUERY);
        Set<Integer> primaries = new HashSet<>();
        for (Endpoint e : endpoints) {
            URI[] uris = e.getURIs();
            assertTrue(uris.length >= 2, "an endpoint without backups: " + Arrays.toString(uris));
            primaries.add(uris[0].getPort());
        }
        assertEquals(3, primaries.size(), "endpoints on every node: " + Arrays.toString(endpoints));
        assertComplete("numbers");
    }

    @Test
    @Order(2)
    void catalogTableNamedLikeAKeywordPlansInPartialMode() throws Exception {
        String name = FlightCatalog.serverName("sparrow", "group");
        assertTrue(plan("SELECT * FROM " + name).length > 1, "not planned in parts: " + name);
        assertComplete("fl.sparrow.`group`");
    }

    @Test
    @Order(3)
    void splitsOfAKilledNodeAreReadFromTheirBackups() throws Exception {
        Endpoint[] before = plan(QUERY);
        TestCluster.Node victim = cluster.node(1);
        victim.killFlight();
        Endpoint[] after = plan(QUERY);
        List<Endpoint> onVictim = new ArrayList<>();
        for (Endpoint e : after) {
            if (cluster.node(e.getURIs()[0]) == victim) {
                onVictim.add(e);
            }
        }
        assertFalse(onVictim.isEmpty(), "the killed node must still get endpoints: " + Arrays.toString(after)
                + ", before: " + Arrays.toString(before));

        // The split reader alone, on exactly the endpoints whose primary is dead.
        RowType rowType = RowType.of(new BigIntType(), new IntType());
        long rows = 0;
        try (FlightSplitReader reader = new FlightSplitReader(config, rowType)) {
            List<FlightSplit> splits = new ArrayList<>();
            for (int i = 0; i < onVictim.size(); i++) {
                splits.add(FlightSplit.of(i, onVictim.get(i)));
            }
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            int finished = 0;
            while (finished < splits.size()) {
                RecordsWithSplitIds<RowData> batch = reader.fetch();
                while (batch.nextSplit() != null) {
                    while (batch.nextRecordFromSplit() != null) {
                        rows++;
                    }
                }
                finished += batch.finishedSplits().size();
            }
        }
        assertTrue(rows > 0, "no rows from the failed-over endpoints");

        // End to end: plans made by the job still send endpoints to the dead node.
        assertComplete("numbers");
        assertComplete("fl.sparrow.`group`");
    }
}
