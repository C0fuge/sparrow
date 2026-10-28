package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Configuration;

@Tag("unit")
class FlightDynamicTableSourceTest {

    private static final Configuration CONFIG = new Configuration("h", 1, "u", null, "t");

    @Test
    void rendersProjectedColumnsQuoted() {
        var source = new FlightDynamicTableSource(CONFIG, "s.t", DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b c", DataTypes.STRING())));
        String sql = source.query();
        assertEquals(true, sql.contains("\"a\"") && sql.contains("\"b c\"") && sql.contains("s.t"), sql);
        source.applyProjection(new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("b c", DataTypes.STRING())));
        assertEquals(false, source.query().contains("\"a\""), source.query());
    }

    @Test
    void emptyProjectionReadsAConstant() {
        var source = new FlightDynamicTableSource(CONFIG, "s.t", DataTypes.ROW());
        assertEquals("SELECT 1 FROM s.t", source.query());
    }

    @Test
    void pushesSupportedFiltersAndLimit() {
        var source = new FlightDynamicTableSource(CONFIG, "s.t", DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("d", DataTypes.DOUBLE())));
        ResolvedExpression onA = CallExpression.permanent(BuiltInFunctionDefinitions.GREATER_THAN,
                List.of(new FieldReferenceExpression("a", DataTypes.INT(), 0, 0), new ValueLiteralExpression(3)),
                DataTypes.BOOLEAN());
        ResolvedExpression onD = CallExpression.permanent(BuiltInFunctionDefinitions.GREATER_THAN,
                List.of(new FieldReferenceExpression("d", DataTypes.DOUBLE(), 0, 1), new ValueLiteralExpression(1.0)),
                DataTypes.BOOLEAN());
        ResolvedExpression notNull = CallExpression.permanent(BuiltInFunctionDefinitions.IS_NOT_NULL,
                List.of(new FieldReferenceExpression("d", DataTypes.DOUBLE(), 0, 1)), DataTypes.BOOLEAN());
        var result = source.applyFilters(List.of(onA, onD, notNull));
        assertEquals(List.of(onA, notNull), result.getAcceptedFilters());
        assertEquals(List.of(onD), result.getRemainingFilters());
        source.applyLimit(10);
        assertEquals("select \"a\",\"d\" from s.t where (\"a\" > 3 and \"d\" is not null) LIMIT 10",
                source.query());
        // copies keep the pushed state; an empty projection keeps the filter
        var copy = (FlightDynamicTableSource) source.copy();
        copy.applyProjection(new int[0][], DataTypes.ROW());
        assertEquals("SELECT 1 FROM s.t WHERE (\"a\" > 3 and \"d\" is not null) LIMIT 10", copy.query());
    }
}
