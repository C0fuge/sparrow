package sparrow.flink;

import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.connector.jdbc.core.database.JdbcFactory;
import org.apache.flink.connector.jdbc.core.database.catalog.JdbcCatalog;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.AbstractDialectConverter;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialectConverter;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

/**
 * Lets Flink's JDBC connector read through the Arrow Flight SQL JDBC driver ({@code jdbc:arrow-flight-sql:}
 * URLs): the perf baseline of {@link TpchPerfTest}. Read-only, DuckDB's SQL: double-quoted identifiers (a
 * {@code schema.table} name quoted part by part), {@code LIMIT n}. Test code only; the connector itself does
 * not use JDBC.
 */
public final class FlightSqlJdbcFactory implements JdbcFactory {

    @Override
    public boolean acceptsURL(String url) {
        return url.startsWith("jdbc:arrow-flight-sql:");
    }

    @Override
    public JdbcDialect createDialect() {
        return new Dialect();
    }

    @Override
    public JdbcCatalog createCatalog(ClassLoader classLoader, String catalogName, String defaultDatabase,
            String username, String pwd, String baseUrl) {
        throw new UnsupportedOperationException("no JDBC catalog for Arrow Flight SQL");
    }

    /** The dialect: standard JDBC value conversion, no writes. */
    static final class Dialect extends AbstractDialect {

        private static final long serialVersionUID = 1L;

        @Override
        public String dialectName() {
            return "ArrowFlightSql";
        }

        @Override
        public Optional<String> defaultDriverName() {
            return Optional.of("org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver");
        }

        @Override
        public JdbcDialectConverter getRowConverter(RowType rowType) {
            return new Converter(rowType);
        }

        @Override
        public String getLimitClause(long limit) {
            return "LIMIT " + limit;
        }

        @Override
        public String quoteIdentifier(String identifier) {
            StringBuilder quoted = new StringBuilder();
            for (String part : identifier.split("\\.", -1)) {
                if (quoted.length() > 0) {
                    quoted.append('.');
                }
                quoted.append('"').append(part.replace("\"", "\"\"")).append('"');
            }
            return quoted.toString();
        }

        @Override
        public Optional<String> getUpsertStatement(String tableName, String[] fieldNames, String[] uniqueKeyFields) {
            return Optional.empty();
        }

        @Override
        public Optional<Range> decimalPrecisionRange() {
            return Optional.of(Range.of(1, 38));
        }

        @Override
        public Optional<Range> timestampPrecisionRange() {
            return Optional.of(Range.of(0, 6));
        }

        @Override
        public Set<LogicalTypeRoot> supportedTypes() {
            return EnumSet.of(LogicalTypeRoot.CHAR, LogicalTypeRoot.VARCHAR, LogicalTypeRoot.BOOLEAN,
                    LogicalTypeRoot.DECIMAL, LogicalTypeRoot.TINYINT, LogicalTypeRoot.SMALLINT,
                    LogicalTypeRoot.INTEGER, LogicalTypeRoot.BIGINT, LogicalTypeRoot.FLOAT, LogicalTypeRoot.DOUBLE,
                    LogicalTypeRoot.DATE, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                    LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
        }
    }

    /** Flink's default JDBC value conversion. */
    static final class Converter extends AbstractDialectConverter {

        private static final long serialVersionUID = 1L;

        /** A converter for rows of {@code rowType}. */
        Converter(RowType rowType) {
            super(rowType);
        }

        @Override
        public String converterName() {
            return "ArrowFlightSql";
        }
    }
}
