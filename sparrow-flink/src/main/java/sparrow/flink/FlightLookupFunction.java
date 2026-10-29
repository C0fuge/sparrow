package sparrow.flink;

import java.io.IOException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.LookupFunction;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.Expr;

/**
 * Lookup (dimension) side of a lookup join: for every key row it plans {@code SELECT <columns> FROM <table>
 * WHERE <pushed filters> AND k1 = v1 AND ...} on the server and reads all returned endpoints (in partial mode one
 * per node) with the same location failover as a scan. Key values are written into the SQL as literals (the
 * server does not bind statement parameters); only key types whose equality the server evaluates exactly as
 * Flink are accepted. A key with a NULL value matches no row (SQL equality), so the server is not asked.
 */
public final class FlightLookupFunction extends LookupFunction {

    private static final long serialVersionUID = 1L;

    private final Configuration config;
    private final String table;
    private final RowType rowType;
    private final List<Expr> filters;
    private final int[] keyIndices;
    private transient RowData.FieldGetter[] getters;

    /**
     * Creates a lookup function.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param rowType produced row type (the columns read)
     * @param filters pushed filters, combined with AND with the key conditions
     * @param keyIndices positions of the key columns in {@code rowType}, in the order of the key row
     */
    FlightLookupFunction(Configuration config, String table, RowType rowType, List<Expr> filters, int[] keyIndices) {
        this.config = config;
        this.table = table;
        this.rowType = rowType;
        this.filters = new ArrayList<>(filters);
        this.keyIndices = keyIndices.clone();
    }

    /**
     * Whether a column type can be a lookup key: types whose equality with a literal has the same result in
     * Flink and in the server's SQL (as for filter pushdown; TIMESTAMP up to microseconds, the server's unit).
     *
     * @param type key column type
     * @return true when supported
     */
    static boolean isKeyType(LogicalType type) {
        return switch (type.getTypeRoot()) {
            case TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, VARCHAR, BOOLEAN, DATE -> true;
            case TIMESTAMP_WITHOUT_TIME_ZONE -> ((TimestampType) type).getPrecision() <= 6;
            default -> false;
        };
    }

    @Override
    public void open(FunctionContext context) throws Exception {
        super.open(context);
        getters = new RowData.FieldGetter[rowType.getFieldCount()];
        for (int i = 0; i < getters.length; i++) {
            getters[i] = RowData.createFieldGetter(rowType.getTypeAt(i), i);
        }
    }

    @Override
    public Collection<RowData> lookup(RowData keyRow) throws IOException {
        String sql = query(keyRow);
        if (sql == null) {
            return List.of();
        }
        List<FlightSplit> splits = FlightSource.plan(config, sql, rowType);
        List<RowData> rows = new ArrayList<>();
        if (splits.isEmpty()) {
            return rows;
        }
        FlightSplitReader reader = new FlightSplitReader(config, rowType);
        try {
            reader.handleSplitsChanges(new SplitsAddition<>(splits));
            int finished = 0;
            while (finished < splits.size()) {
                RecordsWithSplitIds<RowData> records = reader.fetch();
                while (records.nextSplit() != null) {
                    RowData row;
                    while ((row = records.nextRecordFromSplit()) != null) {
                        // A copy: the row is a view of its whole batch, and cached rows outlive the lookup.
                        rows.add(copy(row));
                    }
                }
                finished += records.finishedSplits().size();
            }
        } finally {
            reader.close();
        }
        return rows;
    }

    /**
     * Copies a row into its own memory.
     *
     * @param row row (a view of a columnar batch)
     * @return independent copy
     */
    private RowData copy(RowData row) {
        GenericRowData copy = new GenericRowData(getters.length);
        for (int i = 0; i < getters.length; i++) {
            Object value = getters[i].getFieldOrNull(row);
            if (value instanceof StringData s) {
                value = StringData.fromBytes(s.toBytes());
            } else if (value instanceof byte[] b) {
                value = Arrays.copyOf(b, b.length);
            }
            // DecimalData, TimestampData and boxed primitives are immutable.
            copy.setField(i, value);
        }
        return copy;
    }

    /**
     * Renders the query of one key.
     *
     * @param keyRow key values, in the order of the key columns
     * @return SQL, or null when a key value is NULL (no row can match)
     */
    String query(RowData keyRow) {
        List<Expr> conditions = new ArrayList<>(filters);
        for (int i = 0; i < keyIndices.length; i++) {
            if (keyRow.isNullAt(i)) {
                return null;
            }
            RowType.RowField field = rowType.getFields().get(keyIndices[i]);
            Object value = keyValue(keyRow, i, field.getType());
            Expr.Literal literal = Expr.Literal.of(value).orElseThrow(
                    () -> new IllegalStateException("Unsupported lookup key value " + value));
            conditions.add(new Expr.Compare(Expr.CompareOp.EQ, new Expr.Column(field.getName()), literal));
        }
        return FlightDynamicTableSource.query(table, rowType.getFieldNames(), conditions, -1);
    }

    /**
     * Reads a non-null key value as the Java value of its SQL literal.
     *
     * @param row key row
     * @param pos position in the key row
     * @param type key column type (one {@link #isKeyType} accepts)
     * @return value
     */
    private static Object keyValue(RowData row, int pos, LogicalType type) {
        return switch (type.getTypeRoot()) {
            case TINYINT -> row.getByte(pos);
            case SMALLINT -> row.getShort(pos);
            case INTEGER -> row.getInt(pos);
            case BIGINT -> row.getLong(pos);
            case DECIMAL -> row.getDecimal(pos, ((DecimalType) type).getPrecision(), ((DecimalType) type).getScale())
                    .toBigDecimal();
            case VARCHAR -> row.getString(pos).toString();
            case BOOLEAN -> row.getBoolean(pos);
            case DATE -> LocalDate.ofEpochDay(row.getInt(pos));
            case TIMESTAMP_WITHOUT_TIME_ZONE -> row.getTimestamp(pos, ((TimestampType) type).getPrecision())
                    .toLocalDateTime();
            default -> throw new IllegalStateException("Unsupported lookup key type " + type);
        };
    }
}
