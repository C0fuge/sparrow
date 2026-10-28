package sparrow.flink;

import java.util.Map;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Configuration;

/** Source reader: one fetcher thread reads the splits of this subtask and requests the next when one ends. */
final class FlightSourceReader
        extends SingleThreadMultiplexSourceReaderBase<RowData, RowData, FlightSplit, FlightSplitState> {

    /**
     * Creates a reader.
     *
     * @param config client configuration
     * @param rowType produced row type
     * @param context reader context
     */
    FlightSourceReader(Configuration config, RowType rowType, SourceReaderContext context) {
        super(() -> new FlightSplitReader(config, rowType), new Emitter(), context.getConfiguration(), context);
    }

    @Override
    public void start() {
        if (getNumberOfCurrentlyAssignedSplits() == 0) {
            context.sendSplitRequest();
        }
    }

    @Override
    protected void onSplitFinished(Map<String, FlightSplitState> finishedSplitIds) {
        context.sendSplitRequest();
    }

    @Override
    protected FlightSplitState initializedState(FlightSplit split) {
        return new FlightSplitState(split);
    }

    @Override
    protected FlightSplit toSplitType(String splitId, FlightSplitState state) {
        return state.toSplit();
    }

    /** Emits rows unchanged and counts them in the split state. */
    private static final class Emitter implements RecordEmitter<RowData, RowData, FlightSplitState> {
        @Override
        public void emitRecord(RowData row, org.apache.flink.api.connector.source.SourceOutput<RowData> output,
                FlightSplitState state) {
            output.collect(row);
            state.emitted();
        }
    }
}
