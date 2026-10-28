package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class FlightSplitEnumeratorTest {

    private static FlightSplit split(int i) {
        return new FlightSplit(String.valueOf(i), new byte[] {(byte) i}, new URI[0], 0L);
    }

    @Test
    void plansOnceAndAssignsOnRequest() throws Exception {
        try (var context = new MockSplitEnumeratorContext<FlightSplit>(2)) {
            AtomicInteger plans = new AtomicInteger();
            var e = new FlightSplitEnumerator(context, () -> {
                plans.incrementAndGet();
                return List.of(split(0), split(1));
            }, null);
            e.start();
            context.registerReader(new org.apache.flink.api.connector.source.ReaderInfo(0, "h"));
            context.registerReader(new org.apache.flink.api.connector.source.ReaderInfo(1, "h"));
            e.handleSplitRequest(0, "h");
            e.handleSplitRequest(1, "h");
            e.handleSplitRequest(0, "h");
            assertEquals(List.of(split(0)), context.getSplitsAssignmentSequence().get(0).assignment().get(0));
            assertEquals(List.of(split(1)), context.getSplitsAssignmentSequence().get(1).assignment().get(1));
            assertTrue(context.getSplitsAssignmentSequence().size() == 2);
            assertEquals(1, plans.get());
        }
    }

    @Test
    void restoredEnumeratorDoesNotPlanAgainAndKeepsReturnedSplits() throws Exception {
        try (var context = new MockSplitEnumeratorContext<FlightSplit>(1)) {
            var e = new FlightSplitEnumerator(context, () -> {
                throw new AssertionError("planned again");
            }, new FlightEnumeratorState(true, List.of(split(1))));
            e.start();
            e.addSplitsBack(List.of(split(0)), 0);
            assertEquals(List.of(split(0), split(1)), e.snapshotState(1).pending());
        }
    }
}
