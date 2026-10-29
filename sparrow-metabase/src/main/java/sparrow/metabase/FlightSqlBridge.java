package sparrow.metabase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.arrow.flight.CallOption;
import org.apache.arrow.flight.CallOptions;
import org.apache.arrow.flight.FlightCallHeaders;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.HeaderCallOption;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;

/**
 * Entry point of the Metabase driver's Clojure code: a connection to the server built from a database's
 * connection details, metadata for Metabase's sync ({@code GetDbSchemas}, {@code GetTables} with schemas) and
 * query execution.
 *
 * <p>Queries are always planned in {@code complete} result mode (header {@code x-flight-result-mode: complete}):
 * Metabase sends aggregations, joins, ordering and limits, whose answer only the whole data set gives. In a
 * cluster whose nodes keep their files apart the server refuses such a plan rather than answer partially.
 *
 * <p>Connections come from client core's cache ({@link Client#getOrCreate}), keyed by the configuration, so
 * creating a bridge per call is cheap and nothing needs closing.
 */
public final class FlightSqlBridge {

    /** Request header naming the result mode. */
    static final String RESULT_MODE_HEADER = "x-flight-result-mode";

    /**
     * Netty's switch for {@code sun.misc.Unsafe}. In the shaded jar the literal is relocated with Netty
     * ({@code sparrow.metabase.shaded.io.netty.noUnsafe}), so it only concerns the bundled copy.
     */
    static final String NETTY_NO_UNSAFE = "io.netty.noUnsafe";

    static {
        allowNettyUnsafe(System.getProperties());
    }

    private final Client client;
    private final FlightSqlClient sql;

    /**
     * Wraps a connected client.
     *
     * @param client client (shared, not closed here)
     */
    private FlightSqlBridge(Client client) {
        this.client = client;
        this.sql = new FlightSqlClient(client.getFlightClient());
    }

    /**
     * Connects with a Metabase database's connection details. Keys (others are ignored, blank values count as
     * absent): {@code host} (required), {@code port} (default 8815), {@code user}, {@code password} or
     * {@code token}, {@code tls}, {@code tls-verify} (default true), {@code tls-truststore} and
     * {@code tls-truststore-password}, {@code kerberos-principal} (TLS only, without password/token),
     * {@code default-schema}, {@code max-retries}, {@code timeout-ms} (planning and metadata calls).
     *
     * @param details connection details; values may be strings, numbers or booleans
     * @return a connected bridge
     * @throws IllegalArgumentException if the details are inconsistent
     */
    public static FlightSqlBridge connect(Map<?, ?> details) {
        return new FlightSqlBridge(Client.getOrCreate(configuration(details)));
    }

    /**
     * Builds the client configuration from connection details (see {@link #connect}).
     *
     * @param details connection details
     * @return validated configuration
     * @throws IllegalArgumentException if the details are inconsistent
     */
    static Configuration configuration(Map<?, ?> details) {
        String host = string(details, "host")
                .orElseThrow(() -> new IllegalArgumentException("Connection detail 'host' is required."));
        int port = integer(details, "port", 8815);
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("Connection detail 'port' must be in 1..65535, got " + port + ".");
        }
        String user = string(details, "user").orElse(null);
        String password = string(details, "password").orElse(null);
        String token = string(details, "token").orElse(null);
        if (password != null && token != null) {
            throw new IllegalArgumentException("Set either a password or a token, not both.");
        }
        boolean tls = bool(details, "tls", false);
        boolean verify = bool(details, "tls-verify", true);
        String truststore = string(details, "tls-truststore").orElse(null);
        Configuration config;
        if (truststore == null) {
            config = new Configuration(host, port, tls, verify, user, password, token);
        } else {
            if (!tls || !verify) {
                throw new IllegalArgumentException("A truststore needs TLS with certificate verification.");
            }
            config = new Configuration(host, port, truststore, string(details, "tls-truststore-password")
                    .orElse(null), user, password, token);
            // Client core only logs a warning and trusts nothing when the store cannot be read.
            if (config.getCertificateBytes() == null || config.getCertificateBytes().length == 0) {
                throw new IllegalArgumentException("No trusted certificate could be read from the truststore '"
                        + truststore + "' (check the file and its password).");
            }
        }
        Optional<String> principal = string(details, "kerberos-principal");
        if (principal.isPresent()) {
            if (!tls) {
                throw new IllegalArgumentException("Kerberos authentication is only offered over TLS.");
            }
            if (password != null || token != null) {
                throw new IllegalArgumentException("Set either a Kerberos principal or a password/token, not both.");
            }
            config.setKerberosServicePrincipal(principal.get());
        }
        string(details, "default-schema").ifPresent(config::setDefaultSchema);
        int retries = integer(details, "max-retries", config.getMaxRetries());
        if (retries < 0) {
            throw new IllegalArgumentException("Connection detail 'max-retries' must be >= 0, got " + retries + ".");
        }
        config.setMaxRetries(retries);
        int timeout = integer(details, "timeout-ms", 0);
        if (timeout < 0) {
            throw new IllegalArgumentException("Connection detail 'timeout-ms' must be >= 0, got " + timeout + ".");
        }
        config.setConnectTimeoutMs(timeout);
        return config;
    }

    /**
     * Lets Netty use {@code sun.misc.Unsafe} unless configured otherwise. Netty 4.2 turns it off by default on
     * Java 25+ (Metabase's runtime), and Arrow's Netty allocator then fails with a {@code ClassCastException}
     * ({@code PooledDirectByteBuf} is not a {@code PooledUnsafeDirectByteBuf}) on the first Flight message.
     * Must run before the (bundled) Netty initializes, hence the static initializer of this entry class.
     *
     * @param properties system properties
     */
    static void allowNettyUnsafe(Properties properties) {
        properties.putIfAbsent(NETTY_NO_UNSAFE, "false");
    }

    /**
     * Checks that the server answers a query (Metabase's "can connect").
     *
     * @return true when {@code SELECT 1} returned its row
     */
    public boolean ping() {
        try (QueryResult result = execute("SELECT 1")) {
            return result.hasNext() && ((Number) result.next()[0]).intValue() == 1;
        }
    }

    /**
     * Lists the schemas the user can see.
     *
     * @return schema names, in server order
     */
    public List<String> schemas() {
        return client.retryWithBackoff(() -> {
            List<String> names = new ArrayList<>();
            read(sql.getSchemas(null, null, client.callOptions()), root -> {
                VarCharVector schema = (VarCharVector) root.getVector("db_schema_name");
                for (int r = 0; r < root.getRowCount(); r++) {
                    if (!schema.isNull(r)) {
                        names.add(schema.getObject(r).toString());
                    }
                }
            });
            return names;
        }, "getDbSchemas");
    }

    /**
     * Lists the tables the user can read.
     *
     * @param schema schema name (exact), or null for every schema
     * @return tables, in server order
     */
    public List<TableRef> tables(String schema) {
        return client.retryWithBackoff(() -> {
            List<TableRef> out = new ArrayList<>();
            read(sql.getTables(null, schema, null, null, false, client.callOptions()), root -> {
                VarCharVector schemas = (VarCharVector) root.getVector("db_schema_name");
                VarCharVector names = (VarCharVector) root.getVector("table_name");
                VarCharVector types = (VarCharVector) root.getVector("table_type");
                for (int r = 0; r < root.getRowCount(); r++) {
                    String tableSchema = schemas.isNull(r) ? null : schemas.getObject(r).toString();
                    // Filters are LIKE patterns: keep exact matches only.
                    if (schema != null && !schema.equals(tableSchema)) {
                        continue;
                    }
                    out.add(new TableRef(tableSchema, names.getObject(r).toString(),
                            types == null || types.isNull(r) ? null : types.getObject(r).toString()));
                }
            });
            return out;
        }, "getTables");
    }

    /**
     * Describes one table.
     *
     * @param schema schema name (exact)
     * @param table table name (exact)
     * @return the table's columns, or an empty list when the table does not exist or the user cannot read it
     */
    public List<Column> columns(String schema, String table) {
        return client.retryWithBackoff(() -> {
            List<Column> out = new ArrayList<>();
            read(sql.getTables(null, schema, table, null, true, client.callOptions()), root -> {
                VarCharVector schemas = (VarCharVector) root.getVector("db_schema_name");
                VarCharVector names = (VarCharVector) root.getVector("table_name");
                VarBinaryVector schemaBytes = (VarBinaryVector) root.getVector("table_schema");
                for (int r = 0; r < root.getRowCount(); r++) {
                    if (out.isEmpty() && !schemas.isNull(r) && schema.equals(schemas.getObject(r).toString())
                            && table.equals(names.getObject(r).toString())) {
                        out.addAll(columns(deserialize(schemaBytes.get(r))));
                    }
                }
            });
            return out;
        }, "getTables");
    }

    /**
     * Plans a query in complete result mode and returns its rows, read lazily. The caller closes the result.
     *
     * @param query SQL in DuckDB's dialect
     * @return the result
     */
    public QueryResult execute(String query) {
        FlightInfo info = client.retryWithBackoff(
                () -> sql.execute(query, planOptions()), "execute");
        List<QueryResult.StreamOpener> openers = new ArrayList<>();
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            openers.add(() -> endpoint.getLocations().isEmpty()
                    // Read over the connection that planned the query.
                    ? client.getFlightClient().getStream(endpoint.getTicket(), readOptions())
                    : client.openStream(endpoint));
        }
        return new QueryResult(columns(info.getSchema()), openers);
    }

    /**
     * Returns the columns a query would return, without running it.
     *
     * @param query SQL in DuckDB's dialect
     * @return result columns
     */
    public List<Column> describe(String query) {
        return columns(client.retryWithBackoff(
                () -> sql.getExecuteSchema(query, planOptions()).getSchema(), "getExecuteSchema"));
    }

    /**
     * Options of planning calls: credentials, complete result mode, the client's headers, the timeout.
     *
     * @return call options
     */
    private CallOption[] planOptions() {
        Configuration config = client.getConfig();
        FlightCallHeaders headers = headers(config);
        headers.insert(RESULT_MODE_HEADER, "complete");
        List<CallOption> options = new ArrayList<>(3);
        options.add(client.getBearerToken());
        options.add(new HeaderCallOption(headers));
        if (config.getConnectTimeoutMs() > 0) {
            options.add(CallOptions.timeout(config.getConnectTimeoutMs(), TimeUnit.MILLISECONDS));
        }
        return options.toArray(new CallOption[0]);
    }

    /**
     * Options of result reads: credentials and the client's headers, no timeout (a query may run long;
     * Metabase cancels it through {@link QueryResult#cancel()}).
     *
     * @return call options
     */
    private CallOption[] readOptions() {
        return new CallOption[] {client.getBearerToken(), new HeaderCallOption(headers(client.getConfig()))};
    }

    /**
     * Builds the headers client core sends with every call (default schema, routing).
     *
     * @param config client configuration
     * @return headers
     */
    static FlightCallHeaders headers(Configuration config) {
        FlightCallHeaders headers = new FlightCallHeaders();
        if (config.getDefaultSchema() != null && !config.getDefaultSchema().isEmpty()) {
            headers.insert("SCHEMA", config.getDefaultSchema());
        }
        if (config.getRoutingTag() != null && !config.getRoutingTag().isEmpty()) {
            headers.insert("ROUTING_TAG", config.getRoutingTag());
        }
        if (config.getRoutingQueue() != null && !config.getRoutingQueue().isEmpty()) {
            headers.insert("ROUTING_QUEUE", config.getRoutingQueue());
        }
        return headers;
    }

    /**
     * Converts an Arrow schema into columns.
     *
     * @param schema Arrow schema
     * @return columns
     */
    static List<Column> columns(Schema schema) {
        List<Field> fields = schema.getFields();
        List<Column> out = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            Field f = fields.get(i);
            out.add(new Column(f.getName(), DuckDbTypes.name(f), i, f.isNullable()));
        }
        return out;
    }

    /**
     * Reads every batch of every endpoint of a metadata answer.
     *
     * @param info answer of a metadata call
     * @param consumer called with each batch
     * @throws Exception if a stream fails
     */
    private void read(FlightInfo info, Consumer<VectorSchemaRoot> consumer) throws Exception {
        for (FlightEndpoint endpoint : info.getEndpoints()) {
            try (FlightStream stream = client.openStream(endpoint)) {
                while (stream.next()) {
                    consumer.accept(stream.getRoot());
                }
            }
        }
    }

    /**
     * Deserializes a table schema sent as an IPC schema message.
     *
     * @param bytes message bytes
     * @return schema
     */
    private static Schema deserialize(byte[] bytes) {
        try {
            return MessageSerializer.deserializeSchema(
                    new ReadChannel(Channels.newChannel(new ByteArrayInputStream(bytes))));
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read a table schema sent by the server", e);
        }
    }

    /**
     * Reads a string detail.
     *
     * @param details connection details
     * @param key key
     * @return the trimmed value, empty when absent or blank
     */
    private static Optional<String> string(Map<?, ?> details, String key) {
        Object value = details.get(key);
        if (value == null) {
            return Optional.empty();
        }
        String s = value.toString().trim();
        return s.isEmpty() ? Optional.empty() : Optional.of(s);
    }

    /**
     * Reads an integer detail.
     *
     * @param details connection details
     * @param key key
     * @param fallback value when absent
     * @return the value
     */
    private static int integer(Map<?, ?> details, String key, int fallback) {
        Optional<String> value = string(details, key);
        if (value.isEmpty()) {
            return fallback;
        }
        try {
            return Integer.parseInt(value.get());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Connection detail '" + key + "' must be an integer, got '"
                    + value.get() + "'.", e);
        }
    }

    /**
     * Reads a boolean detail.
     *
     * @param details connection details
     * @param key key
     * @param fallback value when absent
     * @return the value
     */
    private static boolean bool(Map<?, ?> details, String key, boolean fallback) {
        Optional<String> value = string(details, key);
        if (value.isEmpty()) {
            return fallback;
        }
        return switch (value.get().toLowerCase(java.util.Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException("Connection detail '" + key
                    + "' must be true or false, got '" + value.get() + "'.");
        };
    }
}
