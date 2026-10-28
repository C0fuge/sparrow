package sparrow.flink;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.query.TableScan;

/**
 * Table source of a Flight SQL table or query: builds the pushed-down SQL (projection) and reads it with
 * {@link FlightSource}.
 */
final class FlightDynamicTableSource implements ScanTableSource, SupportsProjectionPushDown {

    /** Identifier quote of the server's SQL dialect (DuckDB). */
    static final String QUOTE = "\"";

    private final Configuration config;
    private final String table;
    private DataType producedType;

    /**
     * Creates a source.
     *
     * @param config client configuration
     * @param table table name or SELECT statement
     * @param producedType physical row type declared for the table
     */
    FlightDynamicTableSource(Configuration config, String table, DataType producedType) {
        this.config = config;
        this.table = table;
        this.producedType = producedType;
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
     * Renders the SQL sent to the server for the current projection.
     *
     * @return SQL
     */
    String query() {
        TableScan scan = TableScan.forTable(table, QUOTE);
        List<String> columns = new ArrayList<>(((RowType) producedType.getLogicalType()).getFieldNames());
        if (columns.isEmpty()) {
            // count(*)-style queries project nothing: read one constant column per row.
            return "SELECT 1 FROM " + scan.getSource();
        }
        scan.probe(null, columns, null, null, new pro.surpin.data.arrowflight.client.model.Field[0]);
        return scan.getQueryStatement();
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
        return new FlightDynamicTableSource(config, table, producedType);
    }

    @Override
    public String asSummaryString() {
        return "arrow-flight";
    }
}
