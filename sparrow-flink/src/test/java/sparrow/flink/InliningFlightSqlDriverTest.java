package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/** Parameter inlining of the perf-baseline driver {@link InliningFlightSqlDriver}. */
@Tag("unit")
class InliningFlightSqlDriverTest {

    @Test
    void placeholdersOutsideQuotesBecomeLiterals() throws SQLException {
        Map<Integer, Object> p = new HashMap<>();
        p.put(1, Date.valueOf("1998-09-02"));
        p.put(2, "it's");
        p.put(3, new BigDecimal("0.0600"));
        p.put(4, null);
        p.put(5, 7L);
        assertEquals("SELECT \"a?\" FROM t WHERE d <= DATE '1998-09-02' AND s = 'it''s' AND x = '?'"
                        + " AND n BETWEEN 0.0600 AND NULL OR k = 7",
                InliningFlightSqlDriver.inline("SELECT \"a?\" FROM t WHERE d <= ? AND s = ? AND x = '?'"
                        + " AND n BETWEEN ? AND ? OR k = ?", p));
    }

    @Test
    void literals() throws SQLException {
        assertEquals("TIMESTAMP '2020-01-02 03:04:05.123'",
                InliningFlightSqlDriver.literal(Timestamp.valueOf("2020-01-02 03:04:05.123")));
        assertEquals("CAST(1.5 AS DOUBLE)", InliningFlightSqlDriver.literal(1.5d));
        assertEquals("true", InliningFlightSqlDriver.literal(true));
        assertThrows(SQLException.class, () -> InliningFlightSqlDriver.literal(new Object()));
    }

    @Test
    void missingOrExtraParametersFail() {
        assertThrows(SQLException.class, () -> InliningFlightSqlDriver.inline("a = ? AND b = ?", Map.of(1, 1)));
        assertThrows(SQLException.class, () -> InliningFlightSqlDriver.inline("a = ?", Map.of(1, 1, 2, 2)));
    }
}
