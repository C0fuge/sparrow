package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.lookup.LookupFunctionProvider;
import org.apache.flink.table.connector.source.lookup.PartialCachingLookupProvider;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Configuration;
import pro.surpin.data.arrowflight.client.expr.Expr;

@Tag("unit")
class FlightLookupFunctionTest {

    private static final Configuration CONFIG = new Configuration("h", 1, "u", null, "t");

    private static final DataType ROW = DataTypes.ROW(DataTypes.FIELD("id", DataTypes.BIGINT()),
            DataTypes.FIELD("name", DataTypes.STRING()), DataTypes.FIELD("dec", DataTypes.DECIMAL(5, 2)),
            DataTypes.FIELD("dt", DataTypes.DATE()), DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6)),
            DataTypes.FIELD("d", DataTypes.DOUBLE()));

    @Test
    void rendersKeyConditionsAfterPushedFilters() {
        RowType rowType = (RowType) ROW.getLogicalType();
        List<Expr> filters = List.of(new Expr.Compare(Expr.CompareOp.GT, new Expr.Column("id"),
                Expr.Literal.of(0L).orElseThrow()));
        var f = new FlightLookupFunction(CONFIG, "s.t", rowType, filters, new int[] {1, 2, 3, 4, 0});
        String sql = f.query(GenericRowData.of(StringData.fromString("it's"),
                DecimalData.fromBigDecimal(new BigDecimal("1.50"), 5, 2), (int) LocalDate.of(2024, 2, 29).toEpochDay(),
                TimestampData.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 5000)), 7L));
        assertEquals("select \"id\",\"name\",\"dec\",\"dt\",\"ts\",\"d\" from s.t where (((((\"id\" > 0"
                + " and \"name\" = 'it''s') and \"dec\" = 1.50) and \"dt\" = '2024-02-29')"
                + " and \"ts\" = '2024-01-01 00:00:00.000005') and \"id\" = 7)", sql);
    }

    @Test
    void aNullKeyValueAsksNothing() {
        var f = new FlightLookupFunction(CONFIG, "s.t", (RowType) ROW.getLogicalType(), List.of(), new int[] {0, 1});
        assertNull(f.query(GenericRowData.of(1L, null)));
    }

    @Test
    void keyTypesAreTheExactlyComparableOnes() {
        assertTrue(FlightLookupFunction.isKeyType(DataTypes.TIMESTAMP(6).getLogicalType()));
        assertFalse(FlightLookupFunction.isKeyType(DataTypes.TIMESTAMP(9).getLogicalType()));
        assertFalse(FlightLookupFunction.isKeyType(DataTypes.DOUBLE().getLogicalType()));
        assertFalse(FlightLookupFunction.isKeyType(DataTypes.CHAR(3).getLogicalType()));
        assertFalse(FlightLookupFunction.isKeyType(DataTypes.TIMESTAMP_LTZ(6).getLogicalType()));
        assertTrue(FlightLookupFunction.isKeyType(DataTypes.STRING().getLogicalType()));
    }

    @Test
    void providerHasTheConfiguredCacheAndRejectsInexactKeys() {
        var plain = new FlightDynamicTableSource(CONFIG, "s.t", ROW, null);
        assertFalse(plain.getLookupRuntimeProvider(context(0)) instanceof PartialCachingLookupProvider);
        assertInstanceOf(LookupFunctionProvider.class, plain.getLookupRuntimeProvider(context(0)));
        var cached = new FlightDynamicTableSource(CONFIG, "s.t", ROW, FlightDynamicTableFactory.lookupCache(
                org.apache.flink.configuration.Configuration.fromMap(Map.of("lookup.cache", "PARTIAL",
                        "lookup.partial-cache.max-rows", "10"))));
        assertInstanceOf(PartialCachingLookupProvider.class, cached.getLookupRuntimeProvider(context(1, 0)));
        var e = assertThrows(ValidationException.class, () -> plain.getLookupRuntimeProvider(context(5)));
        assertTrue(e.getMessage().contains("'d'"), e.getMessage());
    }

    @Test
    void cacheOptionsAreValidated() {
        assertNull(FlightDynamicTableFactory.lookupCache(new org.apache.flink.configuration.Configuration()));
        assertThrows(ValidationException.class, () -> FlightDynamicTableFactory.lookupCache(
                org.apache.flink.configuration.Configuration.fromMap(Map.of("lookup.cache", "PARTIAL"))));
        assertThrows(ValidationException.class, () -> FlightDynamicTableFactory.lookupCache(
                org.apache.flink.configuration.Configuration.fromMap(Map.of("lookup.cache", "FULL"))));
    }

    private static LookupTableSource.LookupContext context(int... keys) {
        int[][] k = new int[keys.length][];
        for (int i = 0; i < keys.length; i++) {
            k[i] = new int[] {keys[i]};
        }
        return new LookupTableSource.LookupContext() {
            @Override
            public int[][] getKeys() {
                return k;
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(DataType producedDataType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <T> TypeInformation<T> createTypeInformation(LogicalType producedLogicalType) {
                throw new UnsupportedOperationException();
            }

            @Override
            public DynamicTableSource.DataStructureConverter createDataStructureConverter(DataType producedDataType) {
                throw new UnsupportedOperationException();
            }
        };
    }
}
