package sparrow.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.Expr;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.query.TableScan;

/**
 * Table source of a Flight SQL table or query: builds the pushed-down SQL (projection, filters, limit) and
 * reads it with {@link FlightSource}; as the dimension side of a lookup join it queries the server per key
 * with {@link FlightLookupFunction}, optionally behind Flink's partial lookup cache.
 */
final class FlightDynamicTableSource implements ScanTableSource, LookupTableSource, SupportsProjectionPushDown,
        SupportsFilterPushDown, SupportsLimitPushDown {

    /** Identifier quote of the server's SQL dialect (DuckDB). */
    static final String QUOTE = "\"";

    private final Configuration config;
    private final String table;
    private DataType producedType;
    /** Pushed filters, combined with AND. */
    private final List<Expr> filters;
    /** Pushed row limit, or -1. */
    private long limit;
    /** Cache of lookup results, or null for no cache. */
    private final LookupCache cache;

    /**
     * Creates a source.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param producedType physical row type declared for the table
     * @param cache cache of lookup results, or null for no cache
     */
    FlightDynamicTableSource(Configuration config, String table, DataType producedType, LookupCache cache) {
        this(config, table, producedType, List.of(), -1, cache);
    }

    /**
     * Creates a source with pushed-down state.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param producedType produced row type
     * @param filters pushed filters
     * @param limit pushed limit, or -1
     * @param cache cache of lookup results, or null for no cache
     */
    private FlightDynamicTableSource(Configuration config, String table, DataType producedType, List<Expr> filters,
            long limit, LookupCache cache) {
        this.config = config;
        this.table = table;
        this.producedType = producedType;
        this.filters = new ArrayList<>(filters);
        this.limit = limit;
        this.cache = cache;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext context) {
        return SourceProvider.of(new FlightSource(config, query(), (RowType) producedType.getLogicalType(),
                context.createTypeInformation(producedType)));
    }

    @Override
    public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
        RowType rowType = (RowType) producedType.getLogicalType();
        int[][] keys = context.getKeys();
        int[] keyIndices = new int[keys.length];
        for (int i = 0; i < keys.length; i++) {
            if (keys[i].length != 1) {
                throw new ValidationException("The arrow-flight connector does not support nested lookup keys.");
            }
            keyIndices[i] = keys[i][0];
            RowType.RowField field = rowType.getFields().get(keyIndices[i]);
            if (!FlightLookupFunction.isKeyType(field.getType())) {
                throw new ValidationException("Column '" + field.getName() + "' of type " + field.getType()
                        + " cannot be a lookup key of an arrow-flight table: keys must be TINYINT..BIGINT, DECIMAL,"
                        + " VARCHAR/STRING, BOOLEAN, DATE or TIMESTAMP(p <= 6), whose equality the server evaluates"
                        + " exactly as Flink.");
            }
        }
        // A pushed limit belongs to a scan of the table, not to the rows of one key.
        FlightLookupFunction function = new FlightLookupFunction(config, table, rowType, filters, keyIndices);
        return cache == null ? LookupFunctionProvider.of(function) : PartialCachingLookupProvider.of(function, cache);
    }

    /**
     * Renders the SQL sent to the server for the current projection, filters and limit.
     *
     * @return SQL
     */
    String query() {
        return query(table, ((RowType) producedType.getLogicalType()).getFieldNames(), filters, limit);
    }

    /**
     * Renders the SQL of a scan: the selected columns of a table or query with the given filters and limit.
     *
     * @param table table name or SELECT statement
     * @param columns selected columns, in order (empty: one constant column per row)
     * @param filters conditions combined with AND
     * @param limit row limit, or -1
     * @return SQL
     */
    static String query(String table, List<String> columns, List<Expr> filters, long limit) {
        TableScan scan = TableScan.forTable(table, QUOTE);
        String where = where(scan.renderer(), filters);
        String sql;
        if (columns.isEmpty()) {
            // count(*)-style queries project nothing: read one constant column per row.
            sql = "SELECT 1 FROM " + scan.getSource() + (where == null ? "" : " WHERE " + where);
        } else {
            scan.probe(where, new ArrayList<>(columns), null, null,
                    new pro.surpin.data.arrowflight.client.model.Field[0]);
            sql = scan.getQueryStatement().trim();
        }
        // Flink keeps its own Limit on top: in partial mode every node may return up to `limit` rows.
        return limit >= 0 ? sql + " LIMIT " + limit : sql;
    }

    /**
     * Renders filters as one condition.
     *
     * @param renderer SQL renderer of the server dialect
     * @param filters conditions combined with AND
     * @return WHERE condition, or null without filters
     */
    private static String where(SqlRenderer renderer, List<Expr> filters) {
        Expr all = null;
        for (Expr f : filters) {
            all = all == null ? f : new Expr.And(all, f);
        }
        return all == null ? null : renderer.render(all);
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> expressions) {
        List<ResolvedExpression> accepted = new ArrayList<>();
        List<ResolvedExpression> remaining = new ArrayList<>();
        for (ResolvedExpression e : expressions) {
            var translated = FilterTranslator.translate(e);
            if (translated.isPresent()) {
                filters.add(translated.get());
                accepted.add(e);
            } else {
                remaining.add(e);
            }
        }
        return Result.of(accepted, remaining);
    }

    @Override
    public void applyLimit(long limit) {
        this.limit = limit;
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    @Override
    public void applyProjection(int[][] projectedFields, DataType producedDataType) {
        this.producedType = producedDataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new FlightDynamicTableSource(config, table, producedType, filters, limit, cache);
    }

    @Override
    public String asSummaryString() {
        return "arrow-flight";
    }
}
