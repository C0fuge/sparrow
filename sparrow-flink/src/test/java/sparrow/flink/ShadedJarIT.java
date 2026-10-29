package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.ExceptionUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.server.ServerAuthenticators;

/**
 * The packaged shaded jar ({@code sparrow-flink-<version>-shaded.jar}) as a Flink user would deploy it: this
 * test's class path has no connector classes (failsafe replaces the main classes directory by an empty one), the
 * jar is added with {@code ADD JAR} and loaded by Flink's user-code class loader, while the class path holds the
 * in-process server's unrelocated Arrow, gRPC, Netty, Protobuf, Jackson and BouncyCastle.
 * Reads go to a two-node TLS cluster with a token, so relocated gRPC/Netty TLS (JDK provider, no netty-tcnative)
 * and the truststore conversion (relocated BouncyCastle) are exercised.
 */
@Tag("integration")
class ShadedJarIT {

    private static final String TOKEN = "sparrow-shaded-token";
    private static final long SUM = (long) TestCluster.NUMBERS * (TestCluster.NUMBERS - 1) / 2;
    /** Class-file prefixes allowed in the jar: the connector, client core and the relocated dependencies. */
    private static final List<String> ALLOWED = List.of("sparrow/flink/", "pro/surpin/data/arrowflight/client/");

    private static Path jar;
    private static Path dir;
    private static TestCertificates certs;
    private static TestCluster cluster;

    @BeforeAll
    static void start() throws Exception {
        String path = System.getProperty("sparrow.shaded.jar");
        assertTrue(path != null && Files.isRegularFile(Path.of(path)), "shaded jar not built: " + path);
        jar = Path.of(path);
        dir = Files.createTempDirectory("sparrow-shaded");
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

    private static TableEnvironment env() {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.getConfig().set("parallelism.default", "2");
        env.getConfig().set("restart-strategy.type", "none");
        return env;
    }

    private static TableEnvironment envWithJar() {
        TableEnvironment env = env();
        env.executeSql("ADD JAR '" + jar.toUri() + "'");
        return env;
    }

    private static Map<String, String> tls(Map<String, String> base) {
        Map<String, String> options = new HashMap<>(base);
        options.put("tls", "true");
        options.put("tls.truststore", certs.truststore.toString());
        options.put("tls.truststore.password", TestCertificates.TRUSTSTORE_PASSWORD);
        options.put("token", TOKEN);
        return options;
    }

    private static String with(Map<String, String> options) {
        return options.entrySet().stream().map(e -> "'" + e.getKey() + "' = '" + e.getValue() + "'")
                .collect(Collectors.joining(", "));
    }

    private static List<Row> query(TableEnvironment env, String sql) throws Exception {
        List<Row> rows = new ArrayList<>();
        try (CloseableIterator<Row> it = env.executeSql(sql).collect()) {
            it.forEachRemaining(rows::add);
        }
        return rows;
    }

    private static String read(JarFile file, String name) throws IOException {
        JarEntry entry = file.getJarEntry(name);
        assertTrue(entry != null, "missing " + name);
        try (InputStream in = file.getInputStream(entry)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    void jarHoldsOnlyTheConnectorClientCoreAndRelocatedDependencies() throws Exception {
        Set<String> foreign = new TreeSet<>();
        try (JarFile file = new JarFile(jar.toFile())) {
            file.stream().map(JarEntry::getName).filter(n -> n.endsWith(".class"))
                    .filter(n -> ALLOWED.stream().noneMatch(n::startsWith))
                    .forEach(foreign::add);
            assertTrue(foreign.isEmpty(), "unrelocated classes: " + foreign.stream().limit(20).toList());
            for (String relocated : List.of("org/apache/arrow/flight/FlightClient", "io/grpc/ManagedChannel",
                    "io/netty/buffer/ByteBuf", "com/google/protobuf/Message", "com/fasterxml/jackson/databind/"
                    + "ObjectMapper", "org/bouncycastle/openssl/jcajce/JcaPEMWriter")) {
                assertTrue(file.getJarEntry("sparrow/flink/shaded/" + relocated + ".class") != null, relocated);
            }
            assertTrue(file.getJarEntry("org/apache/flink/connector/base/source/reader/SourceReaderBase.class")
                    == null, "Flink classes belong to the cluster");
            String factories = read(file, "META-INF/services/org.apache.flink.table.factories.Factory");
            assertTrue(factories.contains("sparrow.flink.FlightDynamicTableFactory"), factories);
            assertTrue(factories.contains("sparrow.flink.FlightCatalogFactory"), factories);
            // gRPC finds its transport and name resolvers through relocated service files.
            assertTrue(read(file, "META-INF/services/sparrow.flink.shaded.io.grpc.ManagedChannelProvider")
                    .contains("sparrow.flink.shaded.io.grpc.netty.NettyChannelProvider"));
            assertTrue(read(file, "META-INF/services/sparrow.flink.shaded.io.grpc.NameResolverProvider")
                    .contains("sparrow.flink.shaded.io.grpc."));
        }
    }

    @Test
    void connectorIsNotOnTheTestClassPath() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName("sparrow.flink.FlightSource"));
        TableEnvironment env = env();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH ("
                + with(tls(cluster.options("sparrow.numbers"))) + ")");
        Exception e = assertThrows(Exception.class, () -> query(env, "SELECT COUNT(*) FROM numbers"));
        String text = ExceptionUtils.stringifyException(e);
        assertTrue(text.contains("'arrow-flight'"), text);
    }

    @Test
    void tableReadsBothNodesOverTlsFromTheAddedJar() throws Exception {
        TableEnvironment env = envWithJar();
        env.executeSql("CREATE TABLE numbers (n BIGINT, m INT) WITH ("
                + with(tls(cluster.options("sparrow.numbers"))) + ")");
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, (long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), COUNT(DISTINCT n), SUM(n) FROM numbers"));
        // Filter and limit pushdown run inside the jar too.
        assertEquals(List.of(Row.of(10L)), query(env, "SELECT COUNT(*) FROM (SELECT n FROM numbers WHERE n < 100 "
                + "LIMIT 10)"));
    }

    @Test
    void catalogFromTheAddedJar() throws Exception {
        TableEnvironment env = envWithJar();
        env.executeSql("CREATE CATALOG fl WITH (" + with(tls(cluster.catalogOptions())) + ")");
        assertTrue(query(env, "SHOW TABLES IN fl.sparrow").contains(Row.of("numbers")));
        assertEquals(List.of(Row.of((long) TestCluster.NUMBERS, SUM)),
                query(env, "SELECT COUNT(*), SUM(n) FROM fl.sparrow.numbers"));
    }
}
