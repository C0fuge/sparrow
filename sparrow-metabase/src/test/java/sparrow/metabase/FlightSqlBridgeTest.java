package sparrow.metabase;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Configuration;

/** Connection details and type names. */
@Tag("unit")
class FlightSqlBridgeTest {

    private static Map<String, Object> details(Object... keyValues) {
        Map<String, Object> details = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            details.put((String) keyValues[i], keyValues[i + 1]);
        }
        return details;
    }

    @Test
    void configurationFromDetails() {
        Configuration config = FlightSqlBridge.configuration(details("host", " flight.example ", "port", 9000L,
                "user", "u", "token", "t", "password", "", "tls", true, "tls-verify", "false",
                "default-schema", "tpch", "max-retries", "1", "timeout-ms", 5000, "engine", "ignored"));
        assertEquals("flight.example", config.getFlightHost());
        assertEquals(9000, config.getFlightPort());
        assertEquals("u", config.getUser());
        assertEquals("t", config.getBearerToken());
        assertNull(config.getPassword());
        assertTrue(config.getTlsEnabled());
        assertEquals("tpch", config.getDefaultSchema());
        assertEquals(1, config.getMaxRetries());
        assertEquals(5000, config.getConnectTimeoutMs());
        assertEquals("tpch", FlightSqlBridge.headers(config).get("SCHEMA"));

        Configuration defaults = FlightSqlBridge.configuration(details("host", "h"));
        assertEquals(8815, defaults.getFlightPort());
        assertFalse(defaults.getTlsEnabled());
        assertNull(FlightSqlBridge.headers(defaults).get("SCHEMA"));
    }

    @Test
    void inconsistentDetailsFail() {
        assertThrows(IllegalArgumentException.class, () -> FlightSqlBridge.configuration(details()));
        assertThrows(IllegalArgumentException.class, () -> FlightSqlBridge.configuration(details("host", " ")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "port", 70000)));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "port", "x")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "password", "p", "token", "t")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "tls", "yes")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "tls-truststore", "/x.jks")));
        assertThrows(IllegalArgumentException.class, () -> FlightSqlBridge.configuration(
                details("host", "h", "tls", true, "tls-truststore", "/no/such/store.jks")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "kerberos-principal", "flight/h")));
        assertThrows(IllegalArgumentException.class, () -> FlightSqlBridge.configuration(
                details("host", "h", "tls", true, "kerberos-principal", "flight/h", "token", "t")));
        assertThrows(IllegalArgumentException.class,
                () -> FlightSqlBridge.configuration(details("host", "h", "max-retries", -1)));
    }

    private static Field field(String name, ArrowType type, Field... children) {
        return new Field(name, FieldType.nullable(type), List.of(children));
    }

    @Test
    void duckDbTypeNames() {
        Schema schema = new Schema(List.of(
                field("a", new ArrowType.Int(16, false)),
                field("b", new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)),
                field("c", new ArrowType.Decimal(38, 0, 128)),
                field("d", new ArrowType.Timestamp(TimeUnit.NANOSECOND, null)),
                field("e", new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                field("f", new ArrowType.Timestamp(TimeUnit.SECOND, null)),
                field("g", new ArrowType.Timestamp(TimeUnit.MICROSECOND, "Europe/Berlin")),
                field("h", new ArrowType.Date(org.apache.arrow.vector.types.DateUnit.DAY)),
                field("i", new ArrowType.Interval(org.apache.arrow.vector.types.IntervalUnit.MONTH_DAY_NANO)),
                field("j", ArrowType.LargeUtf8.INSTANCE),
                field("k", new ArrowType.FixedSizeList(3), field("item", ArrowType.Utf8.INSTANCE)),
                field("l", new ArrowType.Map(false), field("entries", ArrowType.Struct.INSTANCE,
                        field("key", ArrowType.Utf8.INSTANCE), field("value", new ArrowType.Int(64, true)))),
                field("m", ArrowType.Struct.INSTANCE, field("q\"t", ArrowType.Bool.INSTANCE))));
        assertEquals(List.of("USMALLINT", "FLOAT", "DECIMAL(38,0)", "TIMESTAMP_NS", "TIMESTAMP_MS", "TIMESTAMP_S",
                "TIMESTAMP WITH TIME ZONE", "DATE", "INTERVAL", "VARCHAR", "VARCHAR[3]", "MAP(VARCHAR, BIGINT)",
                "STRUCT(\"q\"\"t\" BOOLEAN)"),
                FlightSqlBridge.columns(schema).stream().map(Column::databaseType).toList());
    }
}
