package sparrow.flink;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.InstantiationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import pro.surpin.data.arrowflight.client.Client;
import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.Endpoint;
import pro.surpin.data.arrowflight.client.read.EndpointReader;

/**
 * Reads assigned splits one after another in the fetcher thread: one Arrow batch per {@link #fetch()},
 * copied into a Flink columnar batch (rows are {@link ColumnarRowData} views of it) before the next batch replaces
 * the stream's buffers. Opening a split tries its locations in
 * order (primary, then the backups) until one returns its first batch, in up to {@code max-retries + 1} rounds
 * with exponential backoff between rounds; a failure after rows were returned fails the read, since a new
 * stream would replay them.
 */
final class FlightSplitReader implements SplitReader<RowData, FlightSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(FlightSplitReader.class);

    private final Configuration config;
    private final Configuration singleAttempt;
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
        try {
            this.singleAttempt = InstantiationUtil.clone(config);
        } catch (IOException | ClassNotFoundException e) {
            throw new IllegalStateException("Cannot copy the client configuration", e);
        }
        // Retries are rounds over all locations here; one location is tried once per round.
        this.singleAttempt.setMaxRetries(0);
    }

    @Override
    public RecordsWithSplitIds<RowData> fetch() throws IOException {
        boolean batch;
        if (current == null) {
            current = splits.poll();
            if (current == null) {
                return new RecordsBySplits.Builder<RowData>().build();
            }
            batch = open(current);
        } else {
            batch = reader.nextBatch();
        }
        if (batch) {
            return new BatchRecords(current.splitId(), converter.convert(reader.root()));
        }
        RecordsBySplits.Builder<RowData> out = new RecordsBySplits.Builder<>();
        out.addFinishedSplit(current.splitId());
        reader.close();
        reader = null;
        current = null;
        return out.build();
    }

    /**
     * Opens a split at the first location that returns its first batch and keeps that reader.
     *
     * @param split split to open
     * @return whether the first batch has rows (false: the split is empty)
     * @throws IOException if no location could be read in any round
     */
    private boolean open(FlightSplit split) throws IOException {
        URI[] locations = split.locations();
        // No locations: the endpoint is read from the planning server.
        URI[][] targets = locations.length == 0 ? new URI[][] {new URI[0]} : new URI[locations.length][];
        for (int i = 0; i < locations.length; i++) {
            targets[i] = new URI[] {locations[i]};
        }
        Client client = Client.getOrCreate(config);
        IOException last = null;
        for (int round = 0; round <= config.getMaxRetries(); round++) {
            if (round > 0) {
                sleep(config.getRetryBackoffMs() << Math.min(round - 1, 20));
            }
            for (URI[] target : targets) {
                EndpointReader candidate = EndpointReader.forEndpoint(client, singleAttempt,
                        new Endpoint(target, split.ticket()), null);
                try {
                    boolean batch = candidate.nextBatch();
                    reader = candidate;
                    return batch;
                } catch (IOException e) {
                    candidate.close();
                    last = e;
                    LOG.warn("Cannot read {} from {} (round {} of {}): {}", split.splitId(),
                            target.length == 0 ? "the planning server" : target[0], round + 1,
                            config.getMaxRetries() + 1, rootMessage(e));
                }
            }
        }
        throw new IOException("Cannot read " + split + " from any of its locations", last);
    }

    /**
     * Waits between retry rounds.
     *
     * @param millis wait time
     * @throws IOException if interrupted
     */
    private static void sleep(long millis) throws IOException {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting to retry a Flight stream", e);
        }
    }

    /**
     * Returns the message of the innermost cause.
     *
     * @param e exception
     * @return message
     */
    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return t.getMessage();
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

    /** The rows of one columnar batch of one split; each row is its own view, so emitted rows stay valid. */
    private static final class BatchRecords implements RecordsWithSplitIds<RowData> {

        private final VectorizedColumnBatch batch;
        private String splitId;
        private int next;

        /**
         * Creates the records of a batch.
         *
         * @param splitId split the batch belongs to
         * @param batch converted batch
         */
        BatchRecords(String splitId, VectorizedColumnBatch batch) {
            this.splitId = splitId;
            this.batch = batch;
        }

        @Override
        public String nextSplit() {
            String id = splitId;
            splitId = null;
            return id;
        }

        @Override
        public RowData nextRecordFromSplit() {
            return next < batch.getNumRows() ? new ColumnarRowData(batch, next++) : null;
        }

        @Override
        public Set<String> finishedSplits() {
            return Set.of();
        }
    }
}
