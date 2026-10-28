package sparrow.flink;

import java.util.Optional;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;

/**
 * Maps the Arrow type of a server column to the Flink type a catalog table declares for it. Every mapped type
 * is read by {@link ArrowRowConverter} without changing values: unsigned integers get the next wider signed
 * type (UINT64 becomes DECIMAL(20, 0)), timestamps keep their precision, a time zone makes TIMESTAMP_LTZ.
 * Columns are always nullable: the server's nullability is not a guarantee Flink may optimise on.
 */
final class ArrowTypeMapping {

    /** Not instantiable: static helpers only. */
    private ArrowTypeMapping() {
    }

    /**
     * Maps a server column.
     *
     * @param field Arrow field of the column
     * @return the Flink type, or empty when the connector cannot read the Arrow type
     */
    static Optional<DataType> toFlink(Field field) {
        return Optional.ofNullable(map(field.getType()));
    }

    /**
     * Maps an Arrow type.
     *
     * @param type Arrow type
     * @return the Flink type, or null when not supported
     */
    private static DataType map(ArrowType type) {
        if (type instanceof ArrowType.Bool) {
            return DataTypes.BOOLEAN();
        } else if (type instanceof ArrowType.Int i) {
            return integer(i);
        } else if (type instanceof ArrowType.FloatingPoint f) {
            return switch (f.getPrecision()) {
                case SINGLE -> DataTypes.FLOAT();
                case DOUBLE -> DataTypes.DOUBLE();
                case HALF -> null;
            };
        } else if (type instanceof ArrowType.Decimal d) {
            return d.getPrecision() <= DecimalType.MAX_PRECISION && d.getScale() >= 0
                    && d.getScale() <= d.getPrecision() ? DataTypes.DECIMAL(d.getPrecision(), d.getScale()) : null;
        } else if (type instanceof ArrowType.Utf8 || type instanceof ArrowType.LargeUtf8
                || type instanceof ArrowType.Utf8View) {
            return DataTypes.STRING();
        } else if (type instanceof ArrowType.Binary || type instanceof ArrowType.LargeBinary
                || type instanceof ArrowType.BinaryView) {
            return DataTypes.BYTES();
        } else if (type instanceof ArrowType.FixedSizeBinary b) {
            return b.getByteWidth() > 0 ? DataTypes.BINARY(b.getByteWidth()) : null;
        } else if (type instanceof ArrowType.Date) {
            return DataTypes.DATE();
        } else if (type instanceof ArrowType.Time t) {
            // Flink keeps TIME as milliseconds of the day; finer values fail the read instead of being cut.
            return DataTypes.TIME(t.getUnit() == org.apache.arrow.vector.types.TimeUnit.SECOND ? 0 : 3);
        } else if (type instanceof ArrowType.Timestamp ts) {
            int precision = switch (ts.getUnit()) {
                case SECOND -> 0;
                case MILLISECOND -> 3;
                case MICROSECOND -> 6;
                case NANOSECOND -> 9;
            };
            return ts.getTimezone() == null ? DataTypes.TIMESTAMP(precision)
                    : DataTypes.TIMESTAMP_LTZ(precision);
        }
        return null;
    }

    /**
     * Maps an integer type; unsigned types get a signed type that holds every value.
     *
     * @param i Arrow integer type
     * @return Flink type
     */
    private static DataType integer(ArrowType.Int i) {
        int bits = i.getIsSigned() ? i.getBitWidth() : i.getBitWidth() * 2;
        return switch (bits) {
            case 8 -> DataTypes.TINYINT();
            case 16 -> DataTypes.SMALLINT();
            case 32 -> DataTypes.INT();
            case 64 -> DataTypes.BIGINT();
            default -> DataTypes.DECIMAL(20, 0);
        };
    }
}
