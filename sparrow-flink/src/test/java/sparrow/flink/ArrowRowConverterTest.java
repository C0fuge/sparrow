package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeStampMicroTZVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.columnar.ColumnarRowData;
import org.apache.flink.table.data.columnar.vector.VectorizedColumnBatch;
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
            return rows(new ArrowRowConverter(type).convert(root));
        }
    }

    private static RowData[] rows(VectorizedColumnBatch batch) {
        RowData[] rows = new RowData[batch.getNumRows()];
        for (int r = 0; r < rows.length; r++) {
            rows[r] = new ColumnarRowData(batch, r);
        }
        return rows;
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

    @Test
    void unsignedBigintAboveLongMaxIsReadIntoDecimal() {
        UInt8Vector v = new UInt8Vector("c", allocator);
        v.allocateNew(1);
        v.set(0, -1L); // 2^64 - 1
        v.setValueCount(1);
        RowData[] rows = convert(row(DataTypes.DECIMAL(20, 0)), v);
        assertEquals(new BigDecimal("18446744073709551615"), rows[0].getDecimal(0, 20, 0).toBigDecimal());
    }

    @Test
    void timeWithSubMillisecondDigitsFailsInsteadOfBeingCut() {
        TimeMicroVector v = new TimeMicroVector("c", allocator);
        v.allocateNew(2);
        v.set(0, 49_530_123_000L);
        v.set(1, 49_530_123_456L);
        v.setValueCount(2);
        RowType type = row(DataTypes.TIME(3));
        try (VectorSchemaRoot root = new VectorSchemaRoot(List.of(v))) {
            root.setRowCount(2);
            IllegalStateException e = assertThrows(IllegalStateException.class,
                    () -> new ArrowRowConverter(type).convert(root));
            assertTrue(e.getMessage().contains("sub-millisecond"), e.getMessage());
        }
        TimeMicroVector exact = new TimeMicroVector("c", allocator);
        exact.allocateNew(1);
        exact.set(0, 49_530_123_000L);
        exact.setValueCount(1);
        assertEquals(49_530_123, convert(type, exact)[0].getInt(0));
    }

    @Test
    void columnarBatchOutlivesTheArrowBuffers() {
        BitVector bools = new BitVector("b", allocator);
        IntVector ints = new IntVector("i", allocator);
        VarCharVector strings = new VarCharVector("s", allocator);
        LargeVarCharVector large = new LargeVarCharVector("l", allocator);
        VarBinaryVector bins = new VarBinaryVector("y", allocator);
        DecimalVector decimals = new DecimalVector("d", allocator, 12, 2);
        DateDayVector dates = new DateDayVector("t", allocator);
        TimeStampMicroVector stamps = new TimeStampMicroVector("ts", allocator);
        List<FieldVector> vectors = List.of(bools, ints, strings, large, bins, decimals, dates, stamps);
        vectors.forEach(v -> v.setInitialCapacity(3));
        vectors.forEach(FieldVector::allocateNew);
        bools.set(0, 1);
        bools.set(2, 0);
        ints.set(0, 7);
        ints.set(2, -7);
        strings.setSafe(0, "äb".getBytes(StandardCharsets.UTF_8));
        strings.setSafe(2, new byte[0]);
        large.setSafe(0, "x".getBytes(StandardCharsets.UTF_8));
        large.setSafe(2, "yz".getBytes(StandardCharsets.UTF_8));
        bins.setSafe(0, new byte[] {1, 2});
        bins.setSafe(2, new byte[] {3});
        decimals.set(0, new BigDecimal("-1.25"));
        decimals.set(2, new BigDecimal("9999999999.99"));
        dates.set(0, 19_000);
        dates.set(2, -1);
        stamps.set(0, -1L);
        stamps.set(2, 1_000_001L);
        vectors.forEach(v -> v.setNull(1));
        vectors.forEach(v -> v.setValueCount(3));
        RowType type = (RowType) DataTypes.ROW(DataTypes.FIELD("b", DataTypes.BOOLEAN()),
                DataTypes.FIELD("i", DataTypes.BIGINT()), DataTypes.FIELD("s", DataTypes.STRING()),
                DataTypes.FIELD("l", DataTypes.STRING()), DataTypes.FIELD("y", DataTypes.BYTES()),
                DataTypes.FIELD("d", DataTypes.DECIMAL(12, 2)), DataTypes.FIELD("t", DataTypes.DATE()),
                DataTypes.FIELD("ts", DataTypes.TIMESTAMP(6))).getLogicalType();
        RowData[] rows;
        try (VectorSchemaRoot root = new VectorSchemaRoot(vectors)) {
            root.setRowCount(3);
            rows = rows(new ArrowRowConverter(type).convert(root));
        }
        // The Arrow buffers are released here; the rows must not depend on them.
        for (int c = 0; c < 8; c++) {
            assertTrue(rows[1].isNullAt(c), "column " + c);
            assertFalse(rows[0].isNullAt(c), "column " + c);
        }
        assertTrue(rows[0].getBoolean(0));
        assertFalse(rows[2].getBoolean(0));
        assertEquals(7L, rows[0].getLong(1));
        assertEquals(-7L, rows[2].getLong(1));
        assertEquals("äb", rows[0].getString(2).toString());
        assertEquals("", rows[2].getString(2).toString());
        assertEquals("x", rows[0].getString(3).toString());
        assertEquals("yz", rows[2].getString(3).toString());
        assertArrayEquals(new byte[] {1, 2}, rows[0].getBinary(4));
        assertArrayEquals(new byte[] {3}, rows[2].getBinary(4));
        assertEquals(new BigDecimal("-1.25"), rows[0].getDecimal(5, 12, 2).toBigDecimal());
        assertEquals(new BigDecimal("9999999999.99"), rows[2].getDecimal(5, 12, 2).toBigDecimal());
        assertEquals(19_000, rows[0].getInt(6));
        assertEquals(-1, rows[2].getInt(6));
        assertEquals(-1L, rows[0].getTimestamp(7, 6).getMillisecond());
        assertEquals(999_000, rows[0].getTimestamp(7, 6).getNanoOfMillisecond());
        assertEquals(1_000L, rows[2].getTimestamp(7, 6).getMillisecond());
        assertEquals(1_000, rows[2].getTimestamp(7, 6).getNanoOfMillisecond());
    }

    @Test
    void nullRowsAreNotRangeChecked() {
        // A null slot keeps whatever its data buffer holds; that value must not fail the narrowing check.
        BigIntVector v = new BigIntVector("c", allocator);
        v.allocateNew(2);
        v.set(0, 1L << 40);
        v.set(1, 5L);
        v.setNull(0);
        v.setValueCount(2);
        RowData[] rows = convert(row(DataTypes.INT()), v);
        assertTrue(rows[0].isNullAt(0));
        assertEquals(5, rows[1].getInt(0));
    }

    @Test
    void wideDecimalsAndEmptyBatches() {
        DecimalVector v = new DecimalVector("c", allocator, 30, 2);
        v.allocateNew(1);
        v.set(0, new BigDecimal("-123456789012345678901234.56"));
        v.setValueCount(1);
        assertEquals(new BigDecimal("-123456789012345678901234.56"),
                convert(row(DataTypes.DECIMAL(30, 2)), v)[0].getDecimal(0, 30, 2).toBigDecimal());
        DecimalVector narrow = new DecimalVector("c", allocator, 5, 2);
        narrow.allocateNew(1);
        narrow.set(0, new BigDecimal("-123.45"));
        narrow.setValueCount(1);
        assertEquals(new BigDecimal("-123.45"),
                convert(row(DataTypes.DECIMAL(30, 2)), narrow)[0].getDecimal(0, 30, 2).toBigDecimal());
        VarCharVector empty = new VarCharVector("c", allocator);
        empty.allocateNew(0);
        empty.setValueCount(0);
        assertEquals(0, convert(row(DataTypes.STRING()), empty).length);
    }
}
