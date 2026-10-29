package sparrow.metabase;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Names the DuckDB type of an Arrow column, as Metabase stores it in {@code database_type}: the driver maps
 * these names to Metabase base types, and they are what a user writing native SQL sees. The server returns
 * DuckDB results, so every DuckDB type has one Arrow form; the names follow DuckDB's {@code typeof()}.
 */
final class DuckDbTypes {

    /** Not instantiated. */
    private DuckDbTypes() {
    }

    /**
     * Returns the DuckDB type name of a field.
     *
     * @param field Arrow field
     * @return DuckDB type name, e.g. {@code BIGINT}, {@code DECIMAL(10,2)}, {@code TIMESTAMP WITH TIME ZONE},
     *     {@code INTEGER[]}
     */
    static String name(Field field) {
        ArrowType type = field.getType();
        List<Field> children = field.getChildren();
        return switch (type.getTypeID()) {
            case Null -> "NULL";
            case Bool -> "BOOLEAN";
            case Int -> integer((ArrowType.Int) type);
            case FloatingPoint -> ((ArrowType.FloatingPoint) type).getPrecision()
                    == org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE ? "DOUBLE" : "FLOAT";
            case Decimal -> "DECIMAL(" + ((ArrowType.Decimal) type).getPrecision() + ","
                    + ((ArrowType.Decimal) type).getScale() + ")";
            case Utf8, LargeUtf8, Utf8View -> "VARCHAR";
            case Binary, LargeBinary, BinaryView, FixedSizeBinary -> "BLOB";
            case Date -> "DATE";
            case Time -> "TIME";
            case Timestamp -> timestamp((ArrowType.Timestamp) type);
            case Interval, Duration -> "INTERVAL";
            case List, LargeList, ListView, LargeListView -> name(children.get(0)) + "[]";
            case FixedSizeList -> name(children.get(0)) + "[" + ((ArrowType.FixedSizeList) type).getListSize() + "]";
            case Struct -> children.stream().map(c -> "\"" + c.getName().replace("\"", "\"\"") + "\" " + name(c))
                    .collect(Collectors.joining(", ", "STRUCT(", ")"));
            case Map -> {
                List<Field> entry = children.get(0).getChildren();
                yield "MAP(" + name(entry.get(0)) + ", " + name(entry.get(1)) + ")";
            }
            default -> type.toString();
        };
    }

    /**
     * Names an integer type.
     *
     * @param type Arrow integer type
     * @return DuckDB name
     */
    private static String integer(ArrowType.Int type) {
        String name = switch (type.getBitWidth()) {
            case 8 -> "TINYINT";
            case 16 -> "SMALLINT";
            case 32 -> "INTEGER";
            default -> "BIGINT";
        };
        return type.getIsSigned() ? name : "U" + name;
    }

    /**
     * Names a timestamp type (DuckDB's second/millisecond/nanosecond timestamps have their own names).
     *
     * @param type Arrow timestamp type
     * @return DuckDB name
     */
    private static String timestamp(ArrowType.Timestamp type) {
        if (type.getTimezone() != null && !type.getTimezone().isEmpty()) {
            return "TIMESTAMP WITH TIME ZONE";
        }
        return switch (type.getUnit()) {
            case SECOND -> "TIMESTAMP_S";
            case MILLISECOND -> "TIMESTAMP_MS";
            case MICROSECOND -> "TIMESTAMP";
            case NANOSECOND -> "TIMESTAMP_NS";
        };
    }
}
