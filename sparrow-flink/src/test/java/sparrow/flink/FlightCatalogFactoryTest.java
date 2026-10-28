package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.factories.FactoryUtil;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class FlightCatalogFactoryTest {

    private static Catalog create(Map<String, String> options) {
        return FactoryUtil.createCatalog("c", options, new Configuration(),
                FlightCatalogFactoryTest.class.getClassLoader());
    }

    @Test
    void defaultDatabaseFallsBackToTheDefaultSchema() {
        Catalog c = create(Map.of("type", "arrow-flight", "host", "h", "default-schema", "tpch"));
        assertEquals("tpch", c.getDefaultDatabase());
        c = create(Map.of("type", "arrow-flight", "host", "h", "default-schema", "tpch", "default-database", "x"));
        assertEquals("x", c.getDefaultDatabase());
    }

    @Test
    void rejectsMissingDefaultDatabaseAndUnknownOptions() {
        Exception e = assertThrows(ValidationException.class,
                () -> create(Map.of("type", "arrow-flight", "host", "h")));
        assertTrue(messages(e).contains("default-database"));
        assertThrows(ValidationException.class, () -> create(Map.of("type", "arrow-flight", "host", "h",
                "default-database", "x", "tabel", "y")));
    }

    @Test
    void quotesServerNamesThatAreNotPlainIdentifiers() {
        assertEquals("tpch.orders", FlightCatalog.serverName("tpch", "orders"));
        assertEquals("\"my schema\".\"a\"\"b\"", FlightCatalog.serverName("my schema", "a\"b"));
        assertEquals("s.\"1x\"", FlightCatalog.serverName("s", "1x"));
    }

    private static String messages(Throwable t) {
        StringBuilder all = new StringBuilder();
        for (Throwable c = t; c != null; c = c.getCause()) {
            all.append(c.getMessage()).append('\n');
        }
        return all.toString();
    }
}
