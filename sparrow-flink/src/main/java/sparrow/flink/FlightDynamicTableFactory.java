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

/** Factory of {@code 'connector' = 'arrow-flight'} tables. */
public final class FlightDynamicTableFactory implements DynamicTableSourceFactory {

    /** Connector identifier. */
    public static final String IDENTIFIER = "arrow-flight";

    /** Server table ({@code schema.table}) or a SELECT statement to read. */
    public static final ConfigOption<String> TABLE = ConfigOptions.key("table").stringType().noDefaultValue()
            .withDescription("Server table (schema.table) or a SELECT statement.");

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>(FlightOptions.required());
        options.add(TABLE);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return FlightOptions.optional();
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
        return new FlightDynamicTableSource(FlightOptions.toClientConfiguration(helper.getOptions()),
                helper.getOptions().get(TABLE), physical);
    }
}
