package sparrow.flink;

import java.io.File;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.DefaultAllocationManagerOption;
import org.apache.arrow.memory.RootAllocator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;

import pro.surpin.data.arrowflight.server.adapters.ConfigAdapter;
import pro.surpin.data.arrowflight.server.adapters.DuckDbAdapter;
import pro.surpin.data.arrowflight.server.adapters.FlightSessions;
import pro.surpin.data.arrowflight.server.adapters.FlightSqlProducer;
import pro.surpin.data.arrowflight.server.adapters.HazelcastAdapter;
import pro.surpin.data.arrowflight.server.adapters.ParquetAdapter;
import pro.surpin.data.arrowflight.server.adapters.ProducingStreamGate;
import pro.surpin.data.arrowflight.server.adapters.ResultModeMiddleware;
import pro.surpin.data.arrowflight.server.adapters.StreamCompression;
import pro.surpin.data.arrowflight.server.duckdb.DuckDbSetupPlan;
import pro.surpin.data.arrowflight.server.model.AppConfig;
import pro.surpin.data.arrowflight.server.services.ClusterService;
import pro.surpin.data.arrowflight.server.services.ExecutionService;
import pro.surpin.data.arrowflight.server.services.MetadataService;
import pro.surpin.data.arrowflight.server.services.QueryPlanner;
import pro.surpin.data.arrowflight.server.storage.HadoopDataStore;

/**
 * Several in-process Flight SQL server nodes in one Hazelcast cluster over the same generated data (every
 * node holds every file, so a partial-mode plan has one endpoint per node and lists the other nodes as backup
 * locations). Wired as the server's {@code TestFlightServerHelper} does, plus cluster seeds. Data:
 * {@code sparrow.numbers} ({@link #NUMBERS} rows in {@link #FILES} Parquet files) and the same rows in
 * {@code sparrow.group} (an SQL keyword as table name). With a {@link Security}, the nodes serve TLS (and
 * advertise {@code grpc+tls} locations) and authenticate calls as the server does.
 */
final class TestCluster implements AutoCloseable {

    /** Rows of {@code sparrow.numbers}. */
    static final int NUMBERS = 60_000;
    /** Parquet files of each table. */
    static final int FILES = 6;

    private final Path dataDir;
    private final BufferAllocator allocator;
    private final List<Node> nodes = new ArrayList<>();

    /**
     * TLS material and authentication of the nodes, as the server applies them ({@code FlightSecurity}).
     *
     * @param certificate PEM certificate chain, or null for plaintext
     * @param privateKey PEM (PKCS#8) private key, or null for plaintext
     * @param authenticator creates each node's authenticator, or null for none
     */
    record Security(File certificate, File privateKey, Supplier<CallHeaderAuthenticator> authenticator) {
    }

    /** One server node. */
    static final class Node {
        final Location location;
        final HazelcastAdapter hazelcast;
        final ClusterService cluster;
        final FlightSqlProducer producer;
        final FlightServer server;
        final ExecutorService executor = Executors.newCachedThreadPool();
        private boolean stopped;

        private Node(BufferAllocator allocator, int flightPort, int hazelcastPort, String[] seeds, Security security)
                throws Exception {
            System.setProperty("hazelcastPort", String.valueOf(hazelcastPort));
            AppConfig config = ConfigAdapter.getConfig();
            LocalFileSystem fs = new LocalFileSystem();
            fs.initialize(URI.create("file:///"), new Configuration());
            ParquetAdapter parquet = new ParquetAdapter(config, new HadoopDataStore(fs, config.dataDir(), "localhost"));
            DuckDbAdapter duckDb = new DuckDbAdapter(config, executor, DuckDbSetupPlan.of(List.of(), List.of()), 0,
                    Runtime.getRuntime().availableProcessors(), new ProducingStreamGate(0), false);
            MetadataService metadata = new MetadataService(parquet);
            boolean tls = security != null && security.certificate() != null;
            location = tls ? Location.forGrpcTls("localhost", flightPort)
                    : Location.forGrpcInsecure("localhost", flightPort);
            hazelcast = new HazelcastAdapter(config, seeds);
            cluster = new ClusterService(hazelcast, config, location.getUri().toString());
            QueryPlanner planner = new QueryPlanner(parquet, cluster, QueryPlanner.DEFAULT_BACKUP_LOCATIONS,
                    config.endpointsPerServer());
            ExecutionService execution = new ExecutionService(parquet, duckDb, metadata, config, executor);
            producer = new FlightSqlProducer(location, allocator, metadata, planner, execution, cluster, "partial");
            FlightServer.Builder builder = FlightServer.builder(allocator, location, producer)
                    .middleware(ResultModeMiddleware.KEY, new ResultModeMiddleware.Factory())
                    .middleware(StreamCompression.KEY, new StreamCompression.Factory(StreamCompression.Settings.DEFAULT))
                    .middleware(FlightSessions.KEY, new FlightSessions(3600, 1000).factory());
            if (tls) {
                builder.useTls(security.certificate(), security.privateKey());
            }
            if (security != null && security.authenticator() != null) {
                builder.headerAuthenticator(security.authenticator().get());
            }
            server = builder.build();
            server.start();
        }

        /** Flight port of this node. */
        int port() {
            return location.getUri().getPort();
        }

        /**
         * Stops only the Flight server, as a node whose process is gone: its cluster membership and
         * heartbeats stay, so planners keep sending endpoints to it.
         */
        void killFlight() throws InterruptedException {
            if (!stopped) {
                stopped = true;
                server.close();
            }
        }

        private void close() throws Exception {
            killFlight();
            producer.close();
            cluster.close();
            hazelcast.close();
            executor.shutdownNow();
        }
    }

    private TestCluster(Path dataDir, BufferAllocator allocator) {
        this.dataDir = dataDir;
        this.allocator = allocator;
    }

    /** Starts {@code size} nodes over freshly generated data and waits until they see each other. */
    static TestCluster start(int size) throws Exception {
        return start(size, null);
    }

    /** Starts {@code size} nodes with TLS and/or authentication ({@code security} null: neither). */
    static TestCluster start(int size, Security security) throws Exception {
        return start(size, security, TestCluster::writeTables);
    }

    /** Writes a cluster's data as {@code <dir>/<schema>/<table>/*.parquet}. */
    @FunctionalInterface
    interface Data {
        /** Writes the tables under {@code dir}. */
        void write(Path dir) throws Exception;
    }

    /** Starts {@code size} nodes over the tables {@code data} writes instead of the default ones. */
    static TestCluster start(int size, Security security, Data data) throws Exception {
        System.setProperty(DefaultAllocationManagerOption.ALLOCATION_MANAGER_TYPE_PROPERTY_NAME,
                DefaultAllocationManagerOption.AllocationManagerType.Netty.name());
        Path dir = Files.createTempDirectory("sparrow-test-cluster");
        data.write(dir);
        Map<String, String> saved = new HashMap<>();
        for (String key : List.of("dataDir", "hazelcastPort", "duckDbWarmConnections", "duckDbGroups",
                "duckDbThreads", "ioParallelism")) {
            saved.put(key, System.getProperty(key));
        }
        System.setProperty("dataDir", dir.toString());
        System.setProperty("duckDbWarmConnections", "1");
        System.setProperty("duckDbGroups", "1");
        System.setProperty("duckDbThreads", "1");
        System.setProperty("ioParallelism", "2");
        TestCluster cluster = new TestCluster(dir, new RootAllocator(Long.MAX_VALUE));
        try {
            int[] hazelcastPorts = new int[size];
            String[] seeds = new String[size];
            for (int i = 0; i < size; i++) {
                hazelcastPorts[i] = freePort();
                seeds[i] = "127.0.0.1:" + hazelcastPorts[i];
            }
            for (int i = 0; i < size; i++) {
                cluster.nodes.add(new Node(cluster.allocator, freePort(), hazelcastPorts[i], seeds, security));
            }
            cluster.awaitMembers(size);
        } catch (Exception | Error e) {
            cluster.close();
            throw e;
        } finally {
            saved.forEach((k, v) -> {
                if (v == null) {
                    System.clearProperty(k);
                } else {
                    System.setProperty(k, v);
                }
            });
        }
        return cluster;
    }

    /** Directory of the cluster's data, as {@code <schema>/<table>/*.parquet}. */
    Path dataDir() {
        return dataDir;
    }

    /** Node {@code i}; node 0 is the one clients connect to (the planner). */
    Node node(int i) {
        return nodes.get(i);
    }

    /** Node whose Flight location is {@code uri}, or null. */
    Node node(URI uri) {
        for (Node n : nodes) {
            if (n.location.getUri().getPort() == uri.getPort()) {
                return n;
            }
        }
        return null;
    }

    /** Table options of a connector table, connected to node 0. */
    Map<String, String> options(String table) {
        Map<String, String> options = new HashMap<>(Map.of("connector", "arrow-flight", "host", "localhost",
                "port", String.valueOf(node(0).port()), "user", "test-user", "token", "sparrow-test", "table", table));
        // Dead locations are retried; keep the test fast.
        options.put("max-retries", "1");
        return options;
    }

    /** Catalog properties of an {@code arrow-flight} catalog connected to node 0. */
    Map<String, String> catalogOptions() {
        return Map.of("type", "arrow-flight", "host", "localhost", "port", String.valueOf(node(0).port()),
                "user", "test-user", "token", "sparrow-test", "default-database", "sparrow", "max-retries", "1");
    }

    private void awaitMembers(int size) throws InterruptedException {
        long deadline = System.nanoTime() + 60_000_000_000L;
        for (Node n : nodes) {
            while (n.hazelcast.memberCount() != size) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("cluster did not reach " + size + " members");
                }
                Thread.sleep(50);
            }
        }
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** Writes both tables as {@link #FILES} files each; row {@code n} has {@code m = n % 7}. */
    private static void writeTables(Path dir) throws Exception {
        try (Connection c = DriverManager.getConnection("jdbc:duckdb:"); Statement s = c.createStatement()) {
            for (String table : List.of("numbers", "group")) {
                Path tableDir = Files.createDirectories(dir.resolve("sparrow").resolve(table));
                int per = NUMBERS / FILES;
                for (int f = 0; f < FILES; f++) {
                    s.execute("COPY (SELECT range::BIGINT AS n, (range % 7)::INTEGER AS m FROM range(" + f * per
                            + ", " + (f + 1) * per + ")) TO '" + tableDir.resolve("part-" + f + ".parquet")
                            + "' (FORMAT parquet)");
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        Exception first = null;
        for (Node n : nodes) {
            try {
                n.close();
            } catch (Exception e) {
                if (first == null) {
                    first = e;
                }
            }
        }
        allocator.close();
        try (var paths = Files.walk(dataDir)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
        }
        if (first != null) {
            throw first;
        }
    }
}
