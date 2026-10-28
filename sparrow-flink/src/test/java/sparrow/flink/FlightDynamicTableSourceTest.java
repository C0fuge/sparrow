package sparrow.flink;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.flink.table.api.DataTypes;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import pro.surpin.data.arrowflight.client.Configuration;

@Tag("unit")
class FlightDynamicTableSourceTest {

    private static final Configuration CONFIG = new Configuration("h", 1, "u", null, "t");

    @Test
    void rendersProjectedColumnsQuoted() {
        var source = new FlightDynamicTableSource(CONFIG, "s.t", DataTypes.ROW(
                DataTypes.FIELD("a", DataTypes.INT()), DataTypes.FIELD("b c", DataTypes.STRING())));
        String sql = source.query();
        assertEquals(true, sql.contains("\"a\"") && sql.contains("\"b c\"") && sql.contains("s.t"), sql);
        source.applyProjection(new int[][] {{1}}, DataTypes.ROW(DataTypes.FIELD("b c", DataTypes.STRING())));
        assertEquals(false, source.query().contains("\"a\""), source.query());
    }

    @Test
    void emptyProjectionReadsAConstant() {
        var source = new FlightDynamicTableSource(CONFIG, "s.t", DataTypes.ROW());
        assertEquals("SELECT 1 FROM s.t", source.query());
    }
}
