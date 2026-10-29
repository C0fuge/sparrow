package sparrow.flink;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.lookup.LookupOptions;
import org.apache.flink.table.connector.source.lookup.cache.DefaultLookupCache;
import org.apache.flink.table.connector.source.lookup.cache.LookupCache;
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

    /**
     * Flink's lookup cache options (partial cache only); also allowed on catalog tables, set there with an
     * {@code OPTIONS} table hint.
     */
    static final List<ConfigOption<?>> LOOKUP_OPTIONS = List.of(LookupOptions.CACHE_TYPE,
            LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS, LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE,
            LookupOptions.PARTIAL_CACHE_MAX_ROWS, LookupOptions.PARTIAL_CACHE_CACHE_MISSING_KEY);

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
        Set<ConfigOption<?>> options = new HashSet<>(catalogConfig == null ? FlightOptions.optional() : Set.of());
        options.addAll(LOOKUP_OPTIONS);
        return options;
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
        return new FlightDynamicTableSource(config, helper.getOptions().get(TABLE), physical,
                lookupCache(helper.getOptions()));
    }

    /**
     * Creates the lookup cache the options ask for.
     *
     * @param options table options
     * @return the partial cache, or null for {@code 'lookup.cache' = 'NONE'}
     * @throws ValidationException for a full cache or a partial cache without a size or expiry limit
     */
    static LookupCache lookupCache(ReadableConfig options) {
        switch (options.get(LookupOptions.CACHE_TYPE)) {
            case NONE:
                return null;
            case PARTIAL:
                if (options.getOptional(LookupOptions.PARTIAL_CACHE_MAX_ROWS).isEmpty()
                        && options.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE).isEmpty()
                        && options.getOptional(LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS).isEmpty()) {
                    throw new ValidationException("'" + LookupOptions.CACHE_TYPE.key() + "' = 'PARTIAL' needs '"
                            + LookupOptions.PARTIAL_CACHE_MAX_ROWS.key() + "' or an expiry ('"
                            + LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_WRITE.key() + "', '"
                            + LookupOptions.PARTIAL_CACHE_EXPIRE_AFTER_ACCESS.key() + "').");
                }
                return DefaultLookupCache.fromConfig(options);
            default:
                throw new ValidationException("The arrow-flight connector supports '" + LookupOptions.CACHE_TYPE.key()
                        + "' = 'NONE' or 'PARTIAL', got '" + options.get(LookupOptions.CACHE_TYPE) + "'.");
        }
    }
}
