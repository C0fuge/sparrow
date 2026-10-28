package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class FlightSplitSerializerTest {

    private static final FlightSplit SPLIT = new FlightSplit("3", new byte[] {1, 2, 3},
            new URI[] {URI.create("grpc+tcp://a:1"), URI.create("grpc+tcp://b:2")}, 42L);

    @Test
    void splitRoundTrip() throws IOException {
        var ser = FlightSplitSerializer.INSTANCE;
        assertEquals(SPLIT, ser.deserialize(ser.getVersion(), ser.serialize(SPLIT)));
        var empty = new FlightSplit("0", new byte[0], new URI[0], 0L);
        assertEquals(empty, ser.deserialize(ser.getVersion(), ser.serialize(empty)));
    }

    @Test
    void stateRoundTrip() throws IOException {
        var ser = FlightEnumeratorStateSerializer.INSTANCE;
        var state = new FlightEnumeratorState(true, List.of(SPLIT, SPLIT.withRowsEmitted(0)));
        assertEquals(state, ser.deserialize(ser.getVersion(), ser.serialize(state)));
    }

    @Test
    void rejectsUnknownVersion() {
        assertThrows(IOException.class, () -> FlightSplitSerializer.INSTANCE.deserialize(99, new byte[0]));
        assertThrows(IOException.class, () -> FlightEnumeratorStateSerializer.INSTANCE.deserialize(99, new byte[0]));
    }

    @Test
    void splitCopiesArrays() {
        byte[] ticket = {1};
        var split = new FlightSplit("0", ticket, new URI[0], 0L);
        ticket[0] = 9;
        assertEquals(1, split.ticket()[0]);
    }
}
