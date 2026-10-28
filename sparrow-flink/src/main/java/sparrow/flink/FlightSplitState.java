package sparrow.flink;

/** Mutable reader-side state of a split: counts the rows emitted so far for checkpoints. */
final class FlightSplitState {

    private final FlightSplit split;
    private long rowsEmitted;

    /**
     * Creates the state of an assigned split.
     *
     * @param split split
     */
    FlightSplitState(FlightSplit split) {
        this.split = split;
        this.rowsEmitted = split.rowsEmitted();
    }

    /** Counts one emitted row. */
    void emitted() {
        rowsEmitted++;
    }

    /**
     * Returns the split with the current emitted-row count.
     *
     * @return split for a checkpoint
     */
    FlightSplit toSplit() {
        return split.withRowsEmitted(rowsEmitted);
    }
}
