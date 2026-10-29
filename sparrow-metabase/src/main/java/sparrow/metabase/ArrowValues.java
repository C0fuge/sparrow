package sparrow.metabase;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.function.IntFunction;

import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Decimal256Vector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float2Vector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.NullVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMicroVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeNanoVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.UInt1Vector;
import org.apache.arrow.vector.UInt2Vector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VariableWidthFieldVector;
import org.apache.arrow.vector.ViewVarCharVector;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;

/**
 * Reads Arrow cells as the Java values Metabase's query processor handles: {@link Boolean}, {@link Integer}
 * (8/16/32-bit integers), {@link Long}, {@link BigInteger} (UBIGINT), {@link Float}, {@link Double},
 * {@link BigDecimal}, {@link String}, {@code byte[]}, {@link LocalDate}, {@link LocalTime},
 * {@link java.time.LocalDateTime} (timestamps without time zone) and {@link java.time.OffsetDateTime}
 * (timestamps with time zone, in the column's zone). Intervals and nested values (lists, structs, maps) are
 * returned as their text form (JSON for nested values). Nulls are {@code null}.
 */
final class ArrowValues {

    /** Not instantiated. */
    private ArrowValues() {
    }

    /**
     * Returns a reader of one vector's cells. The reader is bound to the vector, which a Flight stream reuses
     * for every batch.
     *
     * @param vector column vector
     * @return row index to value (null for a null cell)
     */
    static IntFunction<Object> reader(FieldVector vector) {
        IntFunction<Object> values = values(vector);
        return row -> vector.isNull(row) ? null : values.apply(row);
    }

    /**
     * Returns the reader of the non-null cells of a vector.
     *
     * @param vector column vector
     * @return row index to value
     */
    private static IntFunction<Object> values(FieldVector vector) {
        if (vector instanceof NullVector) {
            return row -> null;
        } else if (vector instanceof BitVector v) {
            return row -> v.get(row) != 0;
        } else if (vector instanceof TinyIntVector v) {
            return row -> (int) v.get(row);
        } else if (vector instanceof SmallIntVector v) {
            return row -> (int) v.get(row);
        } else if (vector instanceof IntVector v) {
            return v::get;
        } else if (vector instanceof BigIntVector v) {
            return v::get;
        } else if (vector instanceof UInt1Vector v) {
            return row -> Byte.toUnsignedInt(v.get(row));
        } else if (vector instanceof UInt2Vector v) {
            return row -> (int) v.get(row);
        } else if (vector instanceof UInt4Vector v) {
            return row -> Integer.toUnsignedLong(v.get(row));
        } else if (vector instanceof UInt8Vector v) {
            return row -> new BigInteger(Long.toUnsignedString(v.get(row)));
        } else if (vector instanceof Float2Vector v) {
            return row -> v.getValueAsFloat(row);
        } else if (vector instanceof Float4Vector v) {
            return v::get;
        } else if (vector instanceof Float8Vector v) {
            return v::get;
        } else if (vector instanceof DecimalVector v) {
            return v::getObject;
        } else if (vector instanceof Decimal256Vector v) {
            return v::getObject;
        } else if (vector instanceof VarCharVector || vector instanceof LargeVarCharVector
                || vector instanceof ViewVarCharVector) {
            VariableWidthFieldVector v = (VariableWidthFieldVector) vector;
            return row -> new String(v.get(row), StandardCharsets.UTF_8);
        } else if (vector.getField().getType() instanceof ArrowType.Binary
                || vector.getField().getType() instanceof ArrowType.LargeBinary
                || vector.getField().getType() instanceof ArrowType.FixedSizeBinary
                || vector.getField().getType() instanceof ArrowType.BinaryView) {
            return vector::getObject;
        } else if (vector instanceof DateDayVector v) {
            return row -> LocalDate.ofEpochDay(v.get(row));
        } else if (vector instanceof DateMilliVector v) {
            return row -> LocalDate.ofEpochDay(Math.floorDiv(v.get(row), 86_400_000L));
        } else if (vector instanceof TimeSecVector v) {
            return row -> LocalTime.ofSecondOfDay(v.get(row));
        } else if (vector instanceof TimeMilliVector v) {
            return row -> LocalTime.ofNanoOfDay(v.get(row) * 1_000_000L);
        } else if (vector instanceof TimeMicroVector v) {
            return row -> LocalTime.ofNanoOfDay(v.get(row) * 1_000L);
        } else if (vector instanceof TimeNanoVector v) {
            return row -> LocalTime.ofNanoOfDay(v.get(row));
        } else if (vector instanceof TimeStampVector v) {
            return timestamps(v);
        }
        // Intervals, durations and nested values: their text form (nested values print as JSON).
        return row -> {
            Object value = vector.getObject(row);
            return value == null ? null : value.toString();
        };
    }

    /**
     * Reads timestamps: without a time zone as local date-times (the stored wall-clock value), with one as
     * offset date-times in that zone.
     *
     * @param vector timestamp vector of any unit
     * @return row index to value
     */
    private static IntFunction<Object> timestamps(TimeStampVector vector) {
        ArrowType.Timestamp type = (ArrowType.Timestamp) vector.getField().getType();
        ChronoUnit unit = unit(type.getUnit());
        String tz = type.getTimezone();
        ZoneId zone = tz == null || tz.isEmpty() ? null : ZoneId.of(tz);
        return row -> {
            Instant instant = Instant.EPOCH.plus(vector.get(row), unit);
            return zone == null
                    ? instant.atOffset(ZoneOffset.UTC).toLocalDateTime()
                    : instant.atZone(zone).toOffsetDateTime();
        };
    }

    /**
     * Maps an Arrow time unit to a {@link ChronoUnit}.
     *
     * @param unit Arrow unit
     * @return the same unit
     */
    private static ChronoUnit unit(TimeUnit unit) {
        return switch (unit) {
            case SECOND -> ChronoUnit.SECONDS;
            case MILLISECOND -> ChronoUnit.MILLIS;
            case MICROSECOND -> ChronoUnit.MICROS;
            case NANOSECOND -> ChronoUnit.NANOS;
        };
    }
}
