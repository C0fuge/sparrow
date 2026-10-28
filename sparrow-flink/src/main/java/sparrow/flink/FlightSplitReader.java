package sparrow.flink;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.read.EndpointReader;

/**
 * Reads assigned splits one after another in the fetcher thread: one Arrow batch per {@link #fetch()},
 * converted to rows before the next batch replaces the stream's buffers. Opening a stream retries and fails
 * over to the endpoint's backup locations (client core); a failure after rows were returned fails the read.
 */
final class FlightSplitReader implements SplitReader<RowData, FlightSplit> {

    private final Configuration config;
    private final ArrowRowConverter converter;
    private final Deque<FlightSplit> splits = new ArrayDeque<>();
    private FlightSplit current;
    private EndpointReader reader;

    /**
     * Creates a split reader.
     *
     * @param config client configuration
     * @param rowType produced row type
     */
    FlightSplitReader(Configuration config, RowType rowType) {
        this.config = config;
        this.converter = new ArrowRowConverter(rowType);
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        if (current == null) {
            current = splits.poll();
            if (current == null) {
                return new RecordsBySplits.Builder<RowData>().build();
            }
            reader = EndpointReader.forEndpoint(Client.getOrCreate(config), config, current.toEndpoint(), null);
        }
        RecordsBySplits.Builder<RowData> out = new RecordsBySplits.Builder<>();
        if (reader.nextBatch()) {
            for (RowData row : converter.convert(reader.root())) {
                out.add(current.splitId(), row);
            }
        } else {
            out.addFinishedSplit(current.splitId());
            reader.close();
            reader = null;
            current = null;
        }
        return out.build();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<FlightSplit> change) {
        if (!(change instanceof SplitsAddition)) {
            throw new UnsupportedOperationException("Unsupported split change " + change.getClass());
        }
        for (FlightSplit split : change.splits()) {
            if (split.rowsEmitted() > 0) {
                // A Flight stream cannot be resumed at a row offset and its order is not guaranteed to repeat:
                // re-reading it would duplicate or lose rows.
                throw new IllegalStateException("Cannot resume " + split + " after " + split.rowsEmitted()
                        + " emitted rows: Flight streams are not resumable. Run bounded arrow-flight scans in"
                        + " batch mode, or restart the job without restoring this checkpoint.");
            }
            splits.add(split);
        }
    }

    @Override
    public void wakeUp() {
        // fetch() blocks for at most one batch; nothing to interrupt.
    }

    @Override
    public void close() {
        if (reader != null) {
            reader.close();
            reader = null;
        }
    }
}
