package sparrow.flink;

import java.util.HashSet;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.factories.CatalogFactory;
import org.apache.flink.table.factories.FactoryUtil;

/** Factory of {@code 'type' = 'arrow-flight'} catalogs ({@link FlightCatalog}). */
public final class FlightCatalogFactory implements CatalogFactory {

    /** Server schema used when a query names no database; defaults to {@code default-schema}. */
    public static final ConfigOption<String> DEFAULT_DATABASE = ConfigOptions
            .key(CommonCatalogOptions.DEFAULT_DATABASE_KEY).stringType().noDefaultValue()
            .withDescription("Server schema used when a query names no database (default: 'default-schema').");

    @Override
    public String factoryIdentifier() {
        return FlightDynamicTableFactory.IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return FlightOptions.required();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>(FlightOptions.optional());
        options.add(DEFAULT_DATABASE);
        return options;
    }

    @Override
    public Catalog createCatalog(Context context) {
        FactoryUtil.CatalogFactoryHelper helper = FactoryUtil.createCatalogFactoryHelper(this, context);
        helper.validate();
        String database = helper.getOptions().getOptional(DEFAULT_DATABASE)
                .or(() -> helper.getOptions().getOptional(FlightOptions.DEFAULT_SCHEMA))
                .orElseThrow(() -> new ValidationException("Catalog '" + context.getName() + "': set '"
                        + DEFAULT_DATABASE.key() + "' (or '" + FlightOptions.DEFAULT_SCHEMA.key()
                        + "') to a server schema."));
        return new FlightCatalog(context.getName(), database,
                FlightOptions.toClientConfiguration(helper.getOptions()));
    }
}
