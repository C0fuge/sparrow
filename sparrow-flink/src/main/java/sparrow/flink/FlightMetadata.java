package sparrow.flink;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.sql.FlightSqlClient;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ReadChannel;
import org.apache.arrow.vector.ipc.message.MessageSerializer;
import org.apache.arrow.vector.types.pojo.Schema;

import pro.surpin.data.arrowflight.client.Client;

/**
 * Reads the server's metadata with the Flight SQL calls {@code GetDbSchemas} and {@code GetTables} (the latter
 * with the table schemas, Flight SQL's counterpart of JDBC {@code getColumns}). Calls are retried like queries.
 *
 * <p>Name filters are sent as LIKE patterns, where {@code _} and {@code %} are wildcards, so every answer is
 * filtered again by exact name here.
 */
final class FlightMetadata {

    private final Client client;
    private final FlightSqlClient sql;

    /**
     * Creates a metadata reader.
     *
     * @param client connected client (shared, not closed here)
     */
    FlightMetadata(Client client) {
        this.client = client;
        this.sql = new FlightSqlClient(client.getFlightClient());
    }

    /**
     * Lists the server schemas the user can see.
     *
     * @return schema names, in server order
     */
    List<String> schemas() {
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
     * Lists the tables of a schema the user can read.
     *
     * @param schema schema name
     * @return table names, in server order
     */
    List<String> tables(String schema) {
        return new ArrayList<>(tables(schema, null, false).keySet());
    }

    /**
     * Describes one table.
     *
     * @param schema schema name
     * @param table table name
     * @return the columns of {@code SELECT *} from the table, or empty when the table does not exist (or the
     *     user cannot read it)
     */
    Optional<Schema> table(String schema, String table) {
        return Optional.ofNullable(tables(schema, table, true).get(table));
    }

    /**
     * Calls {@code GetTables} for one schema.
     *
     * @param schema schema name (exact)
     * @param table table name (exact), or null for every table
     * @param includeSchema whether to ask for the table schemas
     * @return table name to schema (null values without {@code includeSchema})
     */
    private Map<String, Schema> tables(String schema, String table, boolean includeSchema) {
        return client.retryWithBackoff(() -> {
            Map<String, Schema> out = new LinkedHashMap<>();
            read(sql.getTables(null, schema, table, null, includeSchema, client.callOptions()), root -> {
                VarCharVector schemas = (VarCharVector) root.getVector("db_schema_name");
                VarCharVector names = (VarCharVector) root.getVector("table_name");
                VarBinaryVector schemaBytes = includeSchema ? (VarBinaryVector) root.getVector("table_schema") : null;
                for (int r = 0; r < root.getRowCount(); r++) {
                    String name = names.getObject(r).toString();
                    if (schemas.isNull(r) || !schema.equals(schemas.getObject(r).toString())
                            || table != null && !table.equals(name)) {
                        continue;
                    }
                    out.put(name, schemaBytes == null ? null : deserialize(schemaBytes.get(r)));
                }
            });
            return out;
        }, "getTables");
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
}
