package sparrow.flink;

import java.util.List;

/**
 * Checkpointed state of the split enumerator.
 *
 * @param planned whether the query was planned (its endpoints became splits)
 * @param pending splits not yet assigned to a reader
 */
public record FlightEnumeratorState(boolean planned, List<FlightSplit> pending) {

    /**
     * Copies the split list.
     *
     * @param planned whether the query was planned
     * @param pending unassigned splits
     */
    public FlightEnumeratorState {
        pending = List.copyOf(pending);
    }
}
