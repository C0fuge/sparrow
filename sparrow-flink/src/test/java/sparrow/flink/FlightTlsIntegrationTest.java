package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.server.ServerAuthenticators;

/**
 * A two-node TLS cluster with the server's bearer-token authentication (self-signed certificate generated per
 * run): partial-mode endpoints advertise {@code grpc+tls} locations on both nodes and are read over TLS with the
 * token; a wrong token, an untrusted certificate or a plaintext client fail the job instead of returning rows.
 */
@Tag("integration")
class FlightTlsIntegrationTest {

    private static final String TOKEN = "sparrow-tls-token";
    private static final long SUM = (long) TestCluster.NUMBERS * (TestCluster.NUMBERS - 1) / 2;

    private static Path dir;
    private static TestCertificates certs;
    private static TestCluster cluster;

    @BeforeAll
    static void start() throws Exception {
        dir = Files.createTempDirectory("sparrow-tls");
        certs = TestCertificates.generate(dir);
        cluster = TestCluster.start(2, new TestCluster.Security(certs.certificate, certs.privateKey,
                () -> ServerAuthenticators.bearer(TOKEN)));
    }

    @AfterAll
    static void stop() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
        if (dir != null) {
            try (var paths = Files.walk(dir)) {
                paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
            }
        }
    }

    /** Options of {@code sparrow.numbers} over TLS with the truststore and the right token. */
    private static Map<String, String> tlsOptions() {
        Map<String, String> options = new HashMap<>(cluster.options("sparrow.numbers"));
        options.put("tls", "true");
        options.put("tls.truststore", certs.truststore.toString());
        options.put("tls.truststore.password", TestCertificates.TRUSTSTORE_PASSWORD);
        options.put("token", TOKEN);
        options.put("max-retries", "0");
        return options;
    }

    private static String with(Map<String, String> options) {
        return options.entrySet().stream().map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    private static TableEnvironment env() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("parallelism.default", "2");
        env.getConfig().set("restart-strategy.type", "none");
        return env;
    }

    private static List<Row> query(TableEnvironment env, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(sql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    /** Every row of {@code numbers} exactly once, read through a table with {@code options}. */
    private static void assertComplete(Map<String, String> options) throws Exception {
        TableEnvironment env = env();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH (" + with(options) + ")");
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, (long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), COUNT(DISTINCT n), SUM(n) FROM numbers"));
    }

    /** The job reading {@code numbers} with {@code options} fails; returns the whole failure as text. */
    private static String assertFails(Map<String, String> options) {
        TableEnvironment env = env();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH (" + with(options) + ")");
        Exception e = assertThrows(Exception.class, () -> query(env, "SELECT COUNT(*) FROM numbers"));
        StringWriter text = new StringWriter();
        e.printStackTrace(new PrintWriter(text));
        return text.toString();
    }

    @Test
    void readsBothNodesOverTlsWithTheToken() throws Exception {
        Map<String, String> options = tlsOptions();
        Endpoint[] endpoints = Client.getOrCreate(FlightOptions.toClientConfiguration(
                org.apache.flink.configuration.Configuration.fromMap(options)))
                .getQueryEndpoints("SELECT n, m FROM sparrow.numbers").getEndpoints();
        Set<Integer> primaries = new HashSet<>();
        for (Endpoint e : endpoints) {
            for (URI uri : e.getURIs()) {
                assertEquals("grpc+tls", uri.getScheme(), Arrays.toString(e.getURIs()));
            }
            primaries.add(e.getURIs()[0].getPort());
        }
        assertEquals(2, primaries.size(), "endpoints on both nodes: " + Arrays.toString(endpoints));
        assertComplete(options);
    }

    @Test
    void catalogOverTls() throws Exception {
        Map<String, String> options = new HashMap<>(cluster.catalogOptions());
        options.put("tls", "true");
        options.put("tls.truststore", certs.truststore.toString());
        options.put("tls.truststore.password", TestCertificates.TRUSTSTORE_PASSWORD);
        options.put("token", TOKEN);
        TableEnvironment env = env();
        env.executeSql("CREATE CATALOG fl WITH (" + with(options) + ")");
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), SUM(n) FROM fl.sparrow.numbers"));
    }

    @Test
    void withoutVerificationNoTruststoreIsNeeded() throws Exception {
        Map<String, String> options = tlsOptions();
        options.remove("tls.truststore");
        options.remove("tls.truststore.password");
        options.put("tls.verify", "false");
        assertComplete(options);
    }

    @Test
    void wrongTokenFailsTheJob() {
        Map<String, String> options = tlsOptions();
        options.put("token", "not-the-token");
        String failure = assertFails(options);
        assertTrue(failure.contains("UNAUTHENTICATED") || failure.contains("bearer token"), failure);
    }

    @Test
    void untrustedCertificateFailsTheJob() {
        Map<String, String> options = tlsOptions();
        options.remove("tls.truststore");
        options.remove("tls.truststore.password");
        String failure = assertFails(options);
        assertTrue(failure.contains("SSLHandshakeException") || failure.contains("PKIX")
                || failure.contains("certificate"), failure);
    }

    @Test
    void plaintextClientFailsTheJob() {
        Map<String, String> options = tlsOptions();
        options.remove("tls.truststore");
        options.remove("tls.truststore.password");
        options.put("tls", "false");
        assertFails(options);
    }

    @Test
    void unreadableTruststoreIsAValidationError() {
        Map<String, String> options = tlsOptions();
        options.put("tls.truststore.password", "wrong-password");
        ValidationException e = assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                org.apache.flink.configuration.Configuration.fromMap(options)));
        assertTrue(e.getMessage().contains("tls.truststore"), e.getMessage());
    }
}
