package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
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
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kerby.kerberos.kerb.server.SimpleKdcServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.server.ServerAuthenticators;

/**
 * Kerberos (SPNEGO) authentication against a two-node TLS cluster and an in-process KDC (Apache Kerby; realm,
 * principals and keytabs generated per run). The nodes accept tickets for {@code flight/localhost} with the
 * server's own authenticator and share the session-signing secret; the JVM's Hadoop login user is the client
 * principal, as with Flink's {@code security.kerberos.login.keytab}. The connector exchanges a ticket with the
 * planning node for a signed session and reads endpoints of both nodes with it.
 */
@Tag("integration")
class FlightKerberosIntegrationTest {

    private static final String REALM = "SPARROW.TEST";
    private static final String SERVICE = "flight/localhost@" + REALM;
    private static final String CLIENT = "sparrow-client@" + REALM;
    private static final long SUM = (long) TestCluster.NUMBERS * (TestCluster.NUMBERS - 1) / 2;

    private static Path dir;
    private static Path clientKeytab;
    private static SimpleKdcServer kdc;
    private static String previousKrb5Conf;
    private static TestCertificates certs;
    private static TestCluster cluster;

    @BeforeAll
    static void start() throws Exception {
        dir = Files.createTempDirectory("sparrow-kerberos");
        int port;
        try (ServerSocket socket = new ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        kdc = new SimpleKdcServer();
        kdc.setWorkDir(Files.createDirectories(dir.resolve("kdc")).toFile());
        kdc.setKdcHost("localhost");
        kdc.setKdcRealm(REALM);
        kdc.setAllowUdp(false);
        kdc.setKdcTcpPort(port);
        kdc.init();
        kdc.start();
        Path serviceKeytab = dir.resolve("service.keytab");
        kdc.createAndExportPrincipals(serviceKeytab.toFile(), "flight/localhost");
        clientKeytab = dir.resolve("client.keytab");
        kdc.createAndExportPrincipals(clientKeytab.toFile(), "sparrow-client");
        Path krb5 = dir.resolve("krb5.conf");
        Files.writeString(krb5, "[libdefaults]\n default_realm = " + REALM + "\n"
                + " udp_preference_limit = 1\n dns_lookup_kdc = false\n dns_lookup_realm = false\n"
                + " dns_canonicalize_hostname = false\n rdns = false\n"
                + "[realms]\n " + REALM + " = {\n  kdc = localhost:" + port + "\n }\n");
        previousKrb5Conf = System.setProperty("java.security.krb5.conf", krb5.toString());
        // Earlier tests in this JVM may have loaded the JDK's Kerberos configuration already.
        Class.forName("sun.security.krb5.Config").getMethod("refresh").invoke(null);

        org.apache.hadoop.conf.Configuration hadoop = new org.apache.hadoop.conf.Configuration();
        hadoop.set("hadoop.security.authentication", "kerberos");
        UserGroupInformation.setConfiguration(hadoop);
        UserGroupInformation service = UserGroupInformation.loginUserFromKeytabAndReturnUGI(SERVICE,
                serviceKeytab.toString());
        UserGroupInformation.loginUserFromKeytab(CLIENT, clientKeytab.toString());

        byte[] sessionSecret = new byte[32];
        new SecureRandom().nextBytes(sessionSecret);
        certs = TestCertificates.generate(dir);
        cluster = TestCluster.start(2, new TestCluster.Security(certs.certificate, certs.privateKey,
                () -> ServerAuthenticators.kerberos(service, sessionSecret)));
    }

    @AfterAll
    static void stop() throws Exception {
        try {
            if (cluster != null) {
                cluster.close();
            }
            if (kdc != null) {
                kdc.stop();
            }
        } finally {
            UserGroupInformation.reset();
            UserGroupInformation.setConfiguration(new org.apache.hadoop.conf.Configuration());
            if (previousKrb5Conf == null) {
                System.clearProperty("java.security.krb5.conf");
            } else {
                System.setProperty("java.security.krb5.conf", previousKrb5Conf);
            }
            if (dir != null) {
                try (var paths = Files.walk(dir)) {
                    paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
                }
            }
        }
    }

    /** Kerberos options over TLS (no user, password or token). */
    private static Map<String, String> kerberos(Map<String, String> base) {
        Map<String, String> options = new HashMap<>(base);
        options.remove("user");
        options.remove("token");
        options.put("tls", "true");
        options.put("tls.truststore", certs.truststore.toString());
        options.put("tls.truststore.password", TestCertificates.TRUSTSTORE_PASSWORD);
        options.put("kerberos.service-principal", SERVICE);
        options.put("max-retries", "0");
        return options;
    }

    private static String with(Map<String, String> options) {
        return options.entrySet().stream().map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    /** A batch environment whose Flink cluster logs in from the client keytab (as a secured deployment). */
    private static TableEnvironment env() {
        org.apache.flink.configuration.Configuration conf = new org.apache.flink.configuration.Configuration();
        conf.setString("security.kerberos.login.keytab", clientKeytab.toString());
        conf.setString("security.kerberos.login.principal", CLIENT);
        conf.setString("parallelism.default", "2");
        conf.setString("restart-strategy.type", "none");
        return TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().withConfiguration(conf)
                .build());
    }

    private static List<Row> query(TableEnvironment env, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(sql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    @Test
    void readsBothNodesWithAKerberosSession() throws Exception {
        Map<String, String> options = kerberos(cluster.options("sparrow.numbers"));
        Endpoint[] endpoints = Client.getOrCreate(FlightOptions.toClientConfiguration(
                org.apache.flink.configuration.Configuration.fromMap(options)))
                .getQueryEndpoints("SELECT n, m FROM sparrow.numbers").getEndpoints();
        Set<Integer> primaries = new HashSet<>();
        for (Endpoint e : endpoints) {
            URI primary = e.getURIs()[0];
            assertEquals("grpc+tls", primary.getScheme(), Arrays.toString(e.getURIs()));
            primaries.add(primary.getPort());
        }
        assertEquals(2, primaries.size(), "endpoints on both nodes: " + Arrays.toString(endpoints));

        TableEnvironment env = env();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH (" + with(options) + ")");
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, (long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), COUNT(DISTINCT n), SUM(n) FROM numbers"));
    }

    @Test
    void catalogWithKerberos() throws Exception {
        TableEnvironment env = env();
        env.executeSql("CREATE CATALOG fl WITH (" + with(kerberos(cluster.catalogOptions())) + ")");
        assertTrue(List.of(env.listTables("fl", "sparrow")).contains("numbers"));
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), SUM(n) FROM fl.sparrow.numbers"));
    }

    @Test
    void unknownServicePrincipalFailsTheJob() {
        Map<String, String> options = kerberos(cluster.options("sparrow.numbers"));
        options.put("kerberos.service-principal", "flight/elsewhere@" + REALM);
        TableEnvironment env = env();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH (" + with(options) + ")");
        Exception e = assertThrows(Exception.class, () -> query(env, "SELECT COUNT(*) FROM numbers"));
        StringWriter text = new StringWriter();
        e.printStackTrace(new PrintWriter(text));
        assertTrue(text.toString().contains("Kerberos Flight authentication failed"), text.toString());
    }
}
