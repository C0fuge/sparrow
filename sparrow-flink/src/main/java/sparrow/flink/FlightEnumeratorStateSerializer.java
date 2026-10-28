package sparrow.flink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Serializes {@link FlightEnumeratorState} for checkpoints. */
public final class FlightEnumeratorStateSerializer implements SimpleVersionedSerializer<FlightEnumeratorState> {

    /** Shared instance (the serializer is stateless). */
    public static final FlightEnumeratorStateSerializer INSTANCE = new FlightEnumeratorStateSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FlightEnumeratorState state) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeBoolean(state.planned());
            out.writeInt(state.pending().size());
            for (FlightSplit split : state.pending()) {
                FlightSplitSerializer.write(split, out);
            }
        }
        return bytes.toByteArray();
    }

    @Override
    public FlightEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported FlightEnumeratorState serializer version " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            boolean planned = in.readBoolean();
            int n = in.readInt();
            List<FlightSplit> pending = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                pending.add(FlightSplitSerializer.read(in));
            }
            return new FlightEnumeratorState(planned, pending);
        }
    }
}
