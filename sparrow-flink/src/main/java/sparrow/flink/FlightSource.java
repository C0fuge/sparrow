package sparrow.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.query.QueryEndpoints;

/**
 * Bounded Flink source over one Flight SQL query: the enumerator plans the query when the job starts and
 * every returned endpoint becomes one split, read in parallel by the subtasks.
 */
public final class FlightSource implements Source<RowData, FlightSplit, FlightEnumeratorState>,
        ResultTypeQueryable<RowData> {

    private static final long serialVersionUID = 1L;

    private final Configuration config;
    private final String query;
    private final RowType rowType;
    private final TypeInformation<RowData> producedType;

    /**
     * Creates a source.
     *
     * @param config client configuration
     * @param query SQL sent to the server; its columns must match {@code rowType} by position
     * @param rowType produced row type
     * @param producedType type information of the produced rows
     */
    public FlightSource(Configuration config, String query, RowType rowType, TypeInformation<RowData> producedType) {
        this.config = config;
        this.query = query;
        this.rowType = rowType;
        this.producedType = producedType;
    }

    /**
     * Returns the query sent to the server.
     *
     * @return SQL
     */
    public String query() {
        return query;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FlightSplit, FlightEnumeratorState> createEnumerator(
            SplitEnumeratorContext<FlightSplit> context) {
        return new FlightSplitEnumerator(context, this::plan, null);
    }

    @Override
    public SplitEnumerator<FlightSplit, FlightEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<FlightSplit> context, FlightEnumeratorState state) {
        return new FlightSplitEnumerator(context, this::plan, state);
    }

    @Override
    public SimpleVersionedSerializer<FlightSplit> getSplitSerializer() {
        return FlightSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<FlightEnumeratorState> getEnumeratorCheckpointSerializer() {
        return FlightEnumeratorStateSerializer.INSTANCE;
    }

    @Override
    public SourceReader<RowData, FlightSplit> createReader(SourceReaderContext context) {
        return new FlightSourceReader(config, rowType, context);
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return producedType;
    }

    /**
     * Plans the query on the server.
     *
     * @return one split per endpoint
     */
    private List<FlightSplit> plan() {
        QueryEndpoints planned = Client.getOrCreate(config).getQueryEndpoints(query);
        int columns = planned.getSchema().getFields().size();
        int expected = Math.max(1, rowType.getFieldCount());
        if (columns != expected) {
            throw new IllegalStateException("Query returns " + columns + " columns, the table declares "
                    + expected + ": " + query);
        }
        Endpoint[] endpoints = planned.getEndpoints();
        List<FlightSplit> splits = new ArrayList<>(endpoints.length);
        for (int i = 0; i < endpoints.length; i++) {
            splits.add(FlightSplit.of(i, endpoints[i]));
        }
        return splits;
    }
}
