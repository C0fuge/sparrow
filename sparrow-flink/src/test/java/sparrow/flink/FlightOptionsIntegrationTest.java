package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

/** Options built by the connector connect to the in-process server and plan a query. */
@Tag("integration")
class FlightOptionsIntegrationTest {

    private static TestServer server;

    @BeforeAll
    static void start() throws Exception {
        server = TestServer.start();
    }

    @AfterAll
    static void stop() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void connectsAndPlans() {
        var config = FlightOptions.toClientConfiguration(Configuration.fromMap(Map.of(
                "host", "localhost", "port", String.valueOf(server.port), "user", "test-user",
                "token", "sparrow-options-it", "default-schema", "sparrow")));
        Client client = Client.getOrCreate(config);
        long count = client.execute("SELECT count(*) FROM sparrow.numbers");
        assertTrue(count > 0, "rows: " + count);
        QueryEndpoints qe = client.getQueryEndpoints("SELECT * FROM sparrow.numbers");
        assertTrue(qe.getEndpoints().length >= 1);
        assertEquals(false, qe.getSchema().getFields().isEmpty());
    }
}
