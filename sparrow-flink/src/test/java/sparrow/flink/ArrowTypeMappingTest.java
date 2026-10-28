package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Optional;

import org.apache.arrow.vector.types.DateUnit;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ArrowTypeMappingTest {

    private static Optional<DataType> map(ArrowType type, boolean nullable) {
        return ArrowTypeMapping.toFlink(new Field("c", new FieldType(nullable, type, null), List.of()));
    }

    private static DataType map(ArrowType type) {
        return map(type, true).orElseThrow();
    }

    @Test
    void mapsScalarTypes() {
        assertEquals(DataTypes.BOOLEAN(), map(ArrowType.Bool.INSTANCE));
        assertEquals(DataTypes.TINYINT(), map(new ArrowType.Int(8, true)));
        assertEquals(DataTypes.SMALLINT(), map(new ArrowType.Int(16, true)));
        assertEquals(DataTypes.INT(), map(new ArrowType.Int(32, true)));
        assertEquals(DataTypes.BIGINT(), map(new ArrowType.Int(64, true)));
        assertEquals(DataTypes.FLOAT(), map(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)));
        assertEquals(DataTypes.DOUBLE(), map(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)));
        assertEquals(DataTypes.DECIMAL(15, 2), map(new ArrowType.Decimal(15, 2, 128)));
        assertEquals(DataTypes.DECIMAL(38, 10), map(new ArrowType.Decimal(38, 10, 256)));
        assertEquals(DataTypes.STRING(), map(ArrowType.Utf8.INSTANCE));
        assertEquals(DataTypes.STRING(), map(ArrowType.LargeUtf8.INSTANCE));
        assertEquals(DataTypes.STRING(), map(ArrowType.Utf8View.INSTANCE));
        assertEquals(DataTypes.BYTES(), map(ArrowType.Binary.INSTANCE));
        assertEquals(DataTypes.BINARY(16), map(new ArrowType.FixedSizeBinary(16)));
        assertEquals(DataTypes.DATE(), map(new ArrowType.Date(DateUnit.DAY)));
        assertEquals(DataTypes.DATE(), map(new ArrowType.Date(DateUnit.MILLISECOND)));
        assertEquals(DataTypes.TIME(0), map(new ArrowType.Time(TimeUnit.SECOND, 32)));
        assertEquals(DataTypes.TIME(3), map(new ArrowType.Time(TimeUnit.MICROSECOND, 64)));
        assertEquals(DataTypes.TIMESTAMP(6), map(new ArrowType.Timestamp(TimeUnit.MICROSECOND, null)));
        assertEquals(DataTypes.TIMESTAMP(9), map(new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)));
        assertEquals(DataTypes.TIMESTAMP_LTZ(3), map(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")));
    }

    @Test
    void unsignedIntegersGetATypeHoldingEveryValue() {
        assertEquals(DataTypes.SMALLINT(), map(new ArrowType.Int(8, false)));
        assertEquals(DataTypes.INT(), map(new ArrowType.Int(16, false)));
        assertEquals(DataTypes.BIGINT(), map(new ArrowType.Int(32, false)));
        assertEquals(DataTypes.DECIMAL(20, 0), map(new ArrowType.Int(64, false)));
    }

    @Test
    void columnsAreAlwaysNullable() {
        assertTrue(map(new ArrowType.Int(32, true), false).orElseThrow().getLogicalType().isNullable());
    }

    @Test
    void everyMappedTypeIsReadable() {
        for (ArrowType t : List.of(new ArrowType.Int(64, false), new ArrowType.Decimal(38, 0, 256),
                new ArrowType.Timestamp(TimeUnit.NANOSECOND, "UTC"), new ArrowType.Time(TimeUnit.NANOSECOND, 64),
                new ArrowType.FixedSizeBinary(4))) {
            assertTrue(ArrowRowConverter.isSupported(map(t).getLogicalType()), t.toString());
        }
    }

    @Test
    void unreadableTypesAreNotMapped() {
        assertTrue(map(new ArrowType.FloatingPoint(FloatingPointPrecision.HALF), true).isEmpty());
        assertTrue(map(new ArrowType.Decimal(76, 2, 256), true).isEmpty());
        assertTrue(map(ArrowType.Null.INSTANCE, true).isEmpty());
        assertTrue(map(ArrowType.List.INSTANCE, true).isEmpty());
        assertTrue(map(ArrowType.Struct.INSTANCE, true).isEmpty());
        assertTrue(map(new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.DAY_TIME), true)
                .isEmpty());
    }
}
