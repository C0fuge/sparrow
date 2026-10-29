package sparrow.metabase;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CancellationException;
import java.util.function.IntFunction;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * Rows of one query, read batch by batch from the endpoints of the query's plan in order (a complete answer has
 * one endpoint). Each row is an {@code Object[]} of {@link ArrowValues} values; nothing is read ahead beyond the
 * current Arrow batch. Not thread-safe, except {@link #cancel()}, which another thread may call to stop the
 * query (Metabase does when a user cancels or the query times out).
 */
public final class QueryResult implements Iterator<Object[]>, AutoCloseable {

    /** Opens the stream of one endpoint. */
    @FunctionalInterface
    interface StreamOpener {
        /**
         * Opens the stream.
         *
         * @return the endpoint's stream
         * @throws Exception if it cannot be opened
         */
        FlightStream open() throws Exception;
    }

    private final List<Column> columns;
    private final Deque<StreamOpener> endpoints;
    private volatile FlightStream stream;
    private volatile boolean cancelled;
    private boolean closed;
    private IntFunction<Object>[] readers;
    private int row;
    private int rows;

    /**
     * Creates a result.
     *
     * @param columns columns of the planned schema
     * @param endpoints openers of the endpoints' streams, in plan order
     */
    QueryResult(List<Column> columns, List<StreamOpener> endpoints) {
        this.columns = List.copyOf(columns);
        this.endpoints = new ArrayDeque<>(endpoints);
    }

    /**
     * Returns the result columns.
     *
     * @return columns, in result order
     */
    public List<Column> columns() {
        return columns;
    }

    @Override
    public boolean hasNext() {
        while (row >= rows) {
            if (cancelled) {
                throw new CancellationException("Query cancelled");
            }
            if (closed) {
                return false;
            }
            FlightStream current = stream;
            if (current == null) {
                StreamOpener next = endpoints.poll();
                if (next == null) {
                    closed = true;
                    return false;
                }
                current = open(next);
                stream = current;
                if (cancelled) {
                    current.cancel("Query cancelled", null);
                    continue;
                }
            }
            if (current.next()) {
                bind(current.getRoot());
            } else {
                closeStream();
            }
        }
        return true;
    }

    @Override
    public Object[] next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        Object[] values = new Object[readers.length];
        for (int c = 0; c < values.length; c++) {
            values[c] = readers[c].apply(row);
        }
        row++;
        return values;
    }

    /**
     * Stops the query: the open stream is cancelled on the server, and reading fails with a
     * {@link CancellationException}. May be called from any thread, also after the result was read.
     */
    public void cancel() {
        cancelled = true;
        FlightStream current = stream;
        if (current != null) {
            current.cancel("Query cancelled", null);
        }
    }

    /**
     * Releases the open stream (cancelling it on the server when not read to the end).
     */
    @Override
    public void close() {
        closed = true;
        endpoints.clear();
        rows = 0;
        row = 0;
        closeStream();
    }

    /**
     * Opens an endpoint's stream.
     *
     * @param opener stream opener
     * @return the stream
     */
    private static FlightStream open(StreamOpener opener) {
        try {
            return opener.open();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot read the query result: " + e.getMessage(), e);
        }
    }

    /**
     * Binds the readers to a new batch.
     *
     * @param root the stream's root holding the batch
     */
    @SuppressWarnings("unchecked")
    private void bind(VectorSchemaRoot root) {
        List<FieldVector> vectors = root.getFieldVectors();
        if (vectors.size() != columns.size()) {
            throw new IllegalStateException("The server sent " + vectors.size() + " columns, the plan has "
                    + columns.size());
        }
        readers = new IntFunction[vectors.size()];
        for (int c = 0; c < readers.length; c++) {
            readers[c] = ArrowValues.reader(vectors.get(c));
        }
        row = 0;
        rows = root.getRowCount();
    }

    /**
     * Closes the current stream, if any.
     */
    private void closeStream() {
        FlightStream current = stream;
        stream = null;
        if (current != null) {
            try {
                current.close();
            } catch (Exception e) {
                throw new IllegalStateException("Cannot close the query stream: " + e.getMessage(), e);
            }
        }
    }
}
