package sparrow.flink;

import java.net.URI;
import java.util.Arrays;

import org.apache.flink.api.connector.source.SourceSplit;

import pro.surpin.data.arrowflight.client.query.Endpoint;

/**
 * One Flight endpoint of a planned query: the ticket and its locations (the first location is the primary,
 * the others are backups used for failover). In partial result mode the server returns one endpoint per node.
 */
public final class FlightSplit implements SourceSplit {

    private final String splitId;
    private final byte[] ticket;
    private final URI[] locations;
    private final long rowsEmitted;

    /**
     * Creates a split.
     *
     * @param splitId unique id within the query (the endpoint index)
     * @param ticket Flight ticket
     * @param locations endpoint locations, primary first; empty means the planning server
     * @param rowsEmitted rows of this split already emitted before a checkpoint (0 for a fresh split)
     */
    public FlightSplit(String splitId, byte[] ticket, URI[] locations, long rowsEmitted) {
        this.splitId = splitId;
        this.ticket = ticket.clone();
        this.locations = locations.clone();
        this.rowsEmitted = rowsEmitted;
    }

    /**
     * Creates the split of a planned endpoint.
     *
     * @param index endpoint index in the planned query
     * @param endpoint planned endpoint
     * @return split
     */
    static FlightSplit of(int index, Endpoint endpoint) {
        return new FlightSplit(String.valueOf(index), endpoint.getTicket(), endpoint.getURIs(), 0L);
    }

    @Override
    public String splitId() {
        return splitId;
    }

    /**
     * Returns the Flight ticket.
     *
     * @return a copy of the ticket bytes
     */
    public byte[] ticket() {
        return ticket.clone();
    }

    /**
     * Returns the endpoint locations, primary first.
     *
     * @return a copy of the locations
     */
    public URI[] locations() {
        return locations.clone();
    }

    /**
     * Returns the rows already emitted from this split when it was checkpointed.
     *
     * @return emitted rows, 0 for a split that was not started
     */
    public long rowsEmitted() {
        return rowsEmitted;
    }

    /**
     * Returns a copy with another emitted-row count.
     *
     * @param rows emitted rows
     * @return split
     */
    FlightSplit withRowsEmitted(long rows) {
        return new FlightSplit(splitId, ticket, locations, rows);
    }

    /**
     * Converts the split back into a client endpoint.
     *
     * @return endpoint
     */
    Endpoint toEndpoint() {
        return new Endpoint(locations.clone(), ticket.clone());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof FlightSplit other)) {
            return false;
        }
        return splitId.equals(other.splitId) && rowsEmitted == other.rowsEmitted && Arrays.equals(ticket, other.ticket)
                && Arrays.equals(locations, other.locations);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * (31 * splitId.hashCode() + Arrays.hashCode(ticket)) + Arrays.hashCode(locations))
                + Long.hashCode(rowsEmitted);
    }

    @Override
    public String toString() {
        return "FlightSplit{id=" + splitId + ", locations=" + Arrays.toString(locations) + ", rowsEmitted=" + rowsEmitted
                + "}";
    }
}
