package sparrow.flink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import pro.surpin.data.arrowflight.client.Configuration;

/** Factory of {@code 'connector' = 'arrow-flight'} tables. */
public final class FlightDynamicTableFactory implements DynamicTableSourceFactory {

    /** Connector identifier. */
    public static final String IDENTIFIER = "arrow-flight";

    /** Server table ({@code schema.table}) or a SELECT statement to read. */
    public static final ConfigOption<String> TABLE = ConfigOptions.key("table").stringType().noDefaultValue()
            .withDescription("Server table (schema.table) or a SELECT statement.");

    /** Connection of the {@link FlightCatalog} whose tables this factory creates, or null for DDL tables. */
    private final Configuration catalogConfig;

    /** Creates the factory of DDL tables (found by Flink's factory discovery). */
    public FlightDynamicTableFactory() {
        this(null);
    }

    /**
     * Creates the factory of a catalog's tables: their options name only the server table, the connection is
     * the catalog's (so credentials do not appear in table options).
     *
     * @param catalogConfig the catalog's client configuration, or null for DDL tables
     */
    FlightDynamicTableFactory(Configuration catalogConfig) {
        this.catalogConfig = catalogConfig;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>(catalogConfig == null ? FlightOptions.required() : Set.of());
        options.add(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return catalogConfig == null ? FlightOptions.optional() : Set.of();
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();
        DataType physical = context.getPhysicalRowDataType();
        for (RowType.RowField field : ((RowType) physical.getLogicalType()).getFields()) {
            if (!ArrowRowConverter.isSupported(field.getType())) {
                throw new ValidationException("Column '" + field.getName() + "' has type " + field.getType()
                        + ", which the arrow-flight connector does not support yet.");
            }
        }
        Configuration config = catalogConfig != null ? catalogConfig
                : FlightOptions.toClientConfiguration(helper.getOptions());
        return new FlightDynamicTableSource(config, helper.getOptions().get(TABLE), physical);
    }
}
