package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.expr.SqlRenderer;

@Tag("unit")
class FilterTranslatorTest {

    private static final SqlRenderer RENDERER = new SqlRenderer(FlightDynamicTableSource.QUOTE);

    private static FieldReferenceExpression col(String name, DataType type) {
        return new FieldReferenceExpression(name, type, 0, 0);
    }

    private static ValueLiteralExpression lit(Object value) {
        return new ValueLiteralExpression(value);
    }

    private static ValueLiteralExpression lit(Object value, DataType type) {
        return new ValueLiteralExpression(value, type);
    }

    private static CallExpression call(BuiltInFunctionDefinition f, ResolvedExpression... args) {
        return CallExpression.permanent(f, List.of(args), DataTypes.BOOLEAN());
    }

    private static Optional<String> sql(ResolvedExpression e) {
        return FilterTranslator.translate(e).map(RENDERER::render);
    }

    @Test
    void translatesComparisonsAndMirrorsSwappedOperands() {
        var n = col("n", DataTypes.BIGINT());
        assertEquals(Optional.of("\"n\" = 5"), sql(call(BuiltInFunctionDefinitions.EQUALS, n, lit(5))));
        assertEquals(Optional.of("\"n\" <> 5"), sql(call(BuiltInFunctionDefinitions.NOT_EQUALS, n, lit(5L))));
        assertEquals(Optional.of("\"n\" > 5"), sql(call(BuiltInFunctionDefinitions.LESS_THAN, lit(5), n)));
        assertEquals(Optional.of("\"n\" <= 5"),
                sql(call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL, lit(5), n)));
        assertEquals(Optional.of("\"dec\" >= 1.50"), sql(call(BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                col("dec", DataTypes.DECIMAL(10, 2)), lit(new BigDecimal("1.50")))));
    }

    @Test
    void translatesLogicalOperatorsNullChecksAndIn() {
        var n = col("n", DataTypes.INT());
        var s = col("s", DataTypes.STRING());
        var e = call(BuiltInFunctionDefinitions.OR,
                call(BuiltInFunctionDefinitions.AND, call(BuiltInFunctionDefinitions.GREATER_THAN, n, lit(1)),
                        call(BuiltInFunctionDefinitions.LESS_THAN, n, lit(3)),
                        call(BuiltInFunctionDefinitions.IS_NOT_NULL, s)),
                call(BuiltInFunctionDefinitions.NOT, call(BuiltInFunctionDefinitions.IS_NULL, n)),
                call(BuiltInFunctionDefinitions.IN, s, lit("a"), lit("it's")));
        assertEquals(Optional.of("((((\"n\" > 1 and \"n\" < 3) and \"s\" is not null) or not (\"n\" is null))"
                + " or \"s\" in ('a','it''s'))"), sql(e));
    }

    @Test
    void translatesBooleanDateAndTimestampColumns() {
        assertEquals(Optional.of("\"b\" = true"), sql(col("b", DataTypes.BOOLEAN())));
        assertEquals(Optional.of("\"dt\" < '2024-02-29'"), sql(call(BuiltInFunctionDefinitions.LESS_THAN,
                col("dt", DataTypes.DATE()), lit(LocalDate.of(2024, 2, 29)))));
        assertEquals(Optional.of("\"ts\" = '2024-02-29 13:45:30.123456'"), sql(call(BuiltInFunctionDefinitions.EQUALS,
                col("ts", DataTypes.TIMESTAMP(6)), lit(LocalDateTime.of(2024, 2, 29, 13, 45, 30, 123_456_000)))));
    }

    @Test
    void translatesSimpleLikePatterns() {
        var s = col("s", DataTypes.STRING());
        assertEquals(Optional.of("\"s\" like 'ab%' escape '\\'"), sql(call(BuiltInFunctionDefinitions.LIKE, s,
                lit("ab%"))));
        assertEquals(Optional.of("\"s\" like '%ab' escape '\\'"), sql(call(BuiltInFunctionDefinitions.LIKE, s,
                lit("%ab"))));
        assertEquals(Optional.of("\"s\" like '%a''b%' escape '\\'"), sql(call(BuiltInFunctionDefinitions.LIKE, s,
                lit("%a'b%"))));
        assertEquals(Optional.of("\"s\" like '%' escape '\\'"), sql(call(BuiltInFunctionDefinitions.LIKE, s,
                lit("%"))));
    }

    @Test
    void leavesUnsafePredicatesToFlink() {
        var s = col("s", DataTypes.STRING());
        var n = col("n", DataTypes.INT());
        List<ResolvedExpression> unsupported = List.of(
                // other wildcards, escapes or no wildcard at all
                call(BuiltInFunctionDefinitions.LIKE, s, lit("a_b%")),
                call(BuiltInFunctionDefinitions.LIKE, s, lit("a%b")),
                call(BuiltInFunctionDefinitions.LIKE, s, lit("a\\%")),
                call(BuiltInFunctionDefinitions.LIKE, s, lit("abc")),
                // NaN ordering differs between Flink and the server
                call(BuiltInFunctionDefinitions.GREATER_THAN, col("d", DataTypes.DOUBLE()), lit(1.5d)),
                call(BuiltInFunctionDefinitions.GREATER_THAN, col("f", DataTypes.FLOAT()), lit(1.5f)),
                // nanoseconds would be rounded by the server
                call(BuiltInFunctionDefinitions.EQUALS, col("ts", DataTypes.TIMESTAMP(9)),
                        lit(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 1))),
                // type families differ (Flink would cast)
                call(BuiltInFunctionDefinitions.EQUALS, n, lit("1")),
                call(BuiltInFunctionDefinitions.EQUALS, n, lit(null, DataTypes.INT())),
                call(BuiltInFunctionDefinitions.EQUALS, col("c", DataTypes.CHAR(3)), lit("abc")),
                // column vs column, functions, non-boolean bare column
                call(BuiltInFunctionDefinitions.EQUALS, n, col("m", DataTypes.INT())),
                call(BuiltInFunctionDefinitions.EQUALS, call(BuiltInFunctionDefinitions.UPPER, s), lit("A")),
                n,
                // one unsupported operand makes the whole OR unsupported
                call(BuiltInFunctionDefinitions.OR, call(BuiltInFunctionDefinitions.EQUALS, n, lit(1)),
                        call(BuiltInFunctionDefinitions.LIKE, s, lit("a_"))));
        for (ResolvedExpression e : unsupported) {
            assertTrue(FilterTranslator.translate(e).isEmpty(), e.asSummaryString());
        }
    }
}
