package sparrow.flink;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.function.Supplier;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Plans the query on the server when the job starts (one split per returned endpoint) and hands the splits
 * to readers on request. A restored enumerator does not plan again.
 */
final class FlightSplitEnumerator implements SplitEnumerator<FlightSplit, FlightEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(FlightSplitEnumerator.class);

    private final SplitEnumeratorContext<FlightSplit> context;
    private final Supplier<List<FlightSplit>> planner;
    private final Deque<FlightSplit> pending;
    private boolean planned;

    /**
     * Creates an enumerator.
     *
     * @param context enumerator context
     * @param planner plans the query and returns its splits (called once, on the coordinator)
     * @param restored restored state, or null for a fresh start
     */
    FlightSplitEnumerator(SplitEnumeratorContext<FlightSplit> context, Supplier<List<FlightSplit>> planner,
            FlightEnumeratorState restored) {
        this.context = context;
        this.planner = planner;
        this.pending = new ArrayDeque<>(restored == null ? List.of() : restored.pending());
        this.planned = restored != null && restored.planned();
    }

    @Override
    public void start() {
        if (!planned) {
            List<FlightSplit> splits = planner.get();
            LOG.info("Planned {} Flight splits", splits.size());
            pending.addAll(splits);
            planned = true;
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        FlightSplit split = pending.poll();
        if (split != null) {
            context.assignSplit(split, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }

    @Override
    public void addSplitsBack(List<FlightSplit> splits, int subtaskId) {
        // Splits of a failed reader go back to the front so they are re-read from the start.
        for (int i = splits.size() - 1; i >= 0; i--) {
            pending.addFirst(splits.get(i));
        }
    }

    @Override
    public void addReader(int subtaskId) {
        // Readers pull splits with handleSplitRequest.
    }

    @Override
    public FlightEnumeratorState snapshotState(long checkpointId) {
        return new FlightEnumeratorState(planned, new ArrayList<>(pending));
    }

    @Override
    public void close() {
        // Nothing to release: the client is shared and closed with the JVM.
    }
}
