package sparrow.flink;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/** Serializes {@link FlightSplit}s for checkpoints and for the transfer from the enumerator to the readers. */
public final class FlightSplitSerializer implements SimpleVersionedSerializer<FlightSplit> {

    /** Shared instance (the serializer is stateless). */
    public static final FlightSplitSerializer INSTANCE = new FlightSplitSerializer();

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FlightSplit split) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            write(split, out);
        }
        return bytes.toByteArray();
    }

    @Override
    public FlightSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported FlightSplit serializer version " + version);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(serialized))) {
            return read(in);
        }
    }

    /**
     * Writes a split.
     *
     * @param split split
     * @param out output
     * @throws IOException on write failure
     */
    static void write(FlightSplit split, DataOutputStream out) throws IOException {
        out.writeUTF(split.splitId());
        byte[] ticket = split.ticket();
        out.writeInt(ticket.length);
        out.write(ticket);
        URI[] locations = split.locations();
        out.writeInt(locations.length);
        for (URI uri : locations) {
            out.writeUTF(uri.toString());
        }
        out.writeLong(split.rowsEmitted());
    }

    /**
     * Reads a split written by {@link #write}.
     *
     * @param in input
     * @return split
     * @throws IOException on read failure
     */
    static FlightSplit read(DataInputStream in) throws IOException {
        String id = in.readUTF();
        byte[] ticket = new byte[in.readInt()];
        in.readFully(ticket);
        URI[] locations = new URI[in.readInt()];
        for (int i = 0; i < locations.length; i++) {
            locations[i] = URI.create(in.readUTF());
        }
        return new FlightSplit(id, ticket, locations, in.readLong());
    }
}
