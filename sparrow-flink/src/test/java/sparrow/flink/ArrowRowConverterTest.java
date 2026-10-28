package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class ArrowRowConverterTest {

    private final BufferAllocator allocator = new RootAllocator();

    @AfterEach
    void close() {
        allocator.close();
    }

    private static RowType row(org.apache.flink.table.types.DataType type) {
        return (RowType) DataTypes.ROW(DataTypes.FIELD("c", type)).getLogicalType();
    }

    private RowData[] convert(RowType type, FieldVector vector) {
        try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(vector))) {
            root.setRowCount(vector.getValueCount());
            return new ArrowRowConverter(type).convert(root);
        }
    }

    @Test
    void widensNarrowerServerIntegers() {
        SmallIntVector v = new SmallIntVector("c", allocator);
        v.allocateNew(2);
        v.set(0, -5);
        v.setNull(1);
        v.setValueCount(2);
        RowData[] rows = convert(row(DataTypes.BIGINT()), v);
        assertEquals(-5L, rows[0].getLong(0));
        assertTrue(rows[1].isNullAt(0));
    }

    @Test
    void rejectsIntegerOverflow() {
        BigIntVector v = new BigIntVector("c", allocator);
        v.allocateNew(1);
        v.set(0, 1L << 40);
        v.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.INT()), v));
    }

    @Test
    void rejectsUnsignedBeyondBigint() {
        UInt8Vector v = new UInt8Vector("c", allocator);
        v.allocateNew(1);
        v.set(0, -1L);
        v.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.BIGINT()), v));
    }

    @Test
    void rejectsFloatFromDouble() {
        Float8Vector v = new Float8Vector("c", allocator);
        v.allocateNew(1);
        v.set(0, 0.1);
        v.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.FLOAT()), v));
        Float8Vector w = new Float8Vector("c", allocator);
        w.allocateNew(1);
        w.set(0, 0.1);
        w.setValueCount(1);
        assertEquals(0.1, convert(row(DataTypes.DOUBLE()), w)[0].getDouble(0));
    }

    @Test
    void decimalsKeepEveryDigitOrFail() {
        DecimalVector v = new DecimalVector("c", allocator, 10, 3);
        v.allocateNew(1);
        v.set(0, new BigDecimal("1.250"));
        v.setValueCount(1);
        assertEquals(new BigDecimal("1.25000"),
                convert(row(DataTypes.DECIMAL(8, 5)), v)[0].getDecimal(0, 8, 5).toBigDecimal());
        DecimalVector w = new DecimalVector("c", allocator, 10, 3);
        w.allocateNew(1);
        w.set(0, new BigDecimal("1.255"));
        w.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.DECIMAL(8, 2)), w));
        DecimalVector x = new DecimalVector("c", allocator, 10, 3);
        x.allocateNew(1);
        x.set(0, new BigDecimal("1234.000"));
        x.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.DECIMAL(4, 2)), x));
    }

    @Test
    void timestampZoneMustMatchTheDeclaredType() {
        TimeStampMicroTZVector v = new TimeStampMicroTZVector("c", allocator, "UTC");
        v.allocateNew(1);
        v.set(0, 1_500_001L);
        v.setValueCount(1);
        assertThrows(IllegalStateException.class, () -> convert(row(DataTypes.TIMESTAMP(6)), v));
        TimeStampMicroTZVector w = new TimeStampMicroTZVector("c", allocator, "UTC");
        w.allocateNew(1);
        w.set(0, 1_500_001L);
        w.setValueCount(1);
        var ts = convert(row(DataTypes.TIMESTAMP_LTZ(6)), w)[0].getTimestamp(0, 6);
        assertEquals(1500L, ts.getMillisecond());
        assertEquals(1000, ts.getNanoOfMillisecond());
    }

    @Test
    void rejectsColumnCountMismatchAndUnsupportedTypes() {
        IntVector v = new IntVector("c", allocator);
        v.allocateNew(1);
        v.setValueCount(1);
        RowType two = (RowType) DataTypes.ROW(DataTypes.FIELD("a", DataTypes.INT()),
                DataTypes.FIELD("b", DataTypes.INT())).getLogicalType();
        assertThrows(IllegalStateException.class, () -> convert(two, v));
        assertFalse(ArrowRowConverter.isSupported(DataTypes.ARRAY(DataTypes.INT()).getLogicalType()));
        assertThrows(UnsupportedOperationException.class,
                () -> new ArrowRowConverter(row(DataTypes.ARRAY(DataTypes.INT()))));
    }

    @Test
    void emptyProjectionKeepsTheRowCount() {
        IntVector v = new IntVector("one", allocator);
        v.allocateNew(3);
        v.setValueCount(3);
        RowType none = (RowType) DataTypes.ROW().getLogicalType();
        assertEquals(3, convert(none, v).length);
    }
}
