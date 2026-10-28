package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("unit")
class FlightOptionsTest {

    private static Configuration conf(Map<String, String> m) {
        return Configuration.fromMap(m);
    }

    @Test
    void defaultsAndMapping() {
        var c = FlightOptions.toClientConfiguration(conf(Map.of("host", "h1", "user", "u", "token", "t",
                "default-schema", "s", "connect-timeout", "5 s", "max-retries", "1")));
        assertEquals("h1", c.getFlightHost());
        assertEquals(8815, c.getFlightPort());
        assertEquals("u", c.getUser());
        assertEquals("t", c.getBearerToken());
        assertNull(c.getPassword());
        assertFalse(c.getTlsEnabled());
        assertEquals("s", c.getDefaultSchema());
        assertEquals(5000, c.getConnectTimeoutMs());
        assertEquals(1, c.getMaxRetries());
    }

    @Test
    void tlsAndKerberos() {
        var c = FlightOptions.toClientConfiguration(conf(Map.of("host", "h", "port", "443", "tls", "true",
                "tls.verify", "false", "kerberos.service-principal", "flight/h@REALM")));
        assertTrue(c.getTlsEnabled());
        assertFalse(c.verifyServer());
        assertEquals(443, c.getFlightPort());
        assertEquals("flight/h@REALM", c.getKerberosServicePrincipal());
    }

    @Test
    void rejectsInvalidOptions() {
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(conf(Map.of())));
        assertThrows(ValidationException.class,
                () -> FlightOptions.toClientConfiguration(conf(Map.of("host", "h", "port", "0"))));
        assertThrows(ValidationException.class,
                () -> FlightOptions.toClientConfiguration(conf(Map.of("host", "h", "max-retries", "-1"))));
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                conf(Map.of("host", "h", "password", "p", "token", "t"))));
    }

    @Test
    void kerberosNeedsTlsAndNoPasswordOrToken() {
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                conf(Map.of("host", "h", "kerberos.service-principal", "flight/h@REALM"))));
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(conf(Map.of("host", "h",
                "tls", "true", "token", "t", "kerberos.service-principal", "flight/h@REALM"))));
    }

    @Test
    void truststoreNeedsVerifiedTlsAndAReadableCertificate() {
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                conf(Map.of("host", "h", "tls.truststore", "/x.p12"))));
        assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                conf(Map.of("host", "h", "tls", "true", "tls.verify", "false", "tls.truststore", "/x.p12"))));
        ValidationException e = assertThrows(ValidationException.class, () -> FlightOptions.toClientConfiguration(
                conf(Map.of("host", "h", "tls", "true", "tls.truststore", "/does/not/exist.p12"))));
        assertTrue(e.getMessage().contains("/does/not/exist.p12"), e.getMessage());
    }

    @Test
    void requiredAndOptionalAreDisjoint() {
        assertTrue(FlightOptions.optional().stream().noneMatch(FlightOptions.required()::contains));
    }
}
