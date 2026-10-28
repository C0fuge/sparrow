package sparrow.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsLimitPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.Expr;
import pro.surpin.data.arrowflight.client.expr.SqlRenderer;
import pro.surpin.data.arrowflight.client.query.TableScan;

/**
 * Table source of a Flight SQL table or query: builds the pushed-down SQL (projection, filters, limit) and
 * reads it with {@link FlightSource}.
 */
final class FlightDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown,
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

    /**
     * Creates a source.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param producedType physical row type declared for the table
     */
    FlightDynamicTableSource(Configuration config, String table, DataType producedType) {
        this(config, table, producedType, List.of(), -1);
    }

    /**
     * Creates a source with pushed-down state.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param producedType produced row type
     * @param filters pushed filters
     * @param limit pushed limit, or -1
     */
    private FlightDynamicTableSource(Configuration config, String table, DataType producedType, List<Expr> filters,
            long limit) {
        this.config = config;
        this.table = table;
        this.producedType = producedType;
        this.filters = new ArrayList<>(filters);
        this.limit = limit;
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

    /**
     * Renders the SQL sent to the server for the current projection, filters and limit.
     *
     * @return SQL
     */
    String query() {
        TableScan scan = TableScan.forTable(table, QUOTE);
        String where = where(scan.renderer());
        List<String> columns = new ArrayList<>(((RowType) producedType.getLogicalType()).getFieldNames());
        String sql;
        if (columns.isEmpty()) {
            // count(*)-style queries project nothing: read one constant column per row.
            sql = "SELECT 1 FROM " + scan.getSource() + (where == null ? "" : " WHERE " + where);
        } else {
            scan.probe(where, columns, null, null, new pro.surpin.data.arrowflight.client.model.Field[0]);
            sql = scan.getQueryStatement().trim();
        }
        // Flink keeps its own Limit on top: in partial mode every node may return up to `limit` rows.
        return limit >= 0 ? sql + " LIMIT " + limit : sql;
    }

    /**
     * Renders the pushed filters as one condition.
     *
     * @param renderer SQL renderer of the server dialect
     * @return WHERE condition, or null without filters
     */
    private String where(SqlRenderer renderer) {
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
        return new FlightDynamicTableSource(config, table, producedType, filters, limit);
    }

    @Override
    public String asSummaryString() {
        return "arrow-flight";
    }
}
