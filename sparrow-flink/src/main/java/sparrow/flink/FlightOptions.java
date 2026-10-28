package sparrow.flink;

import java.time.Duration;
import java.util.Set;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import pro.surpin.data.arrowflight.client.Configuration;

/**
 * Connection options of the Flink connector ({@code 'connector' = 'arrow-flight'} table options and catalog
 * properties) and their translation into the engine-neutral client {@link Configuration}.
 */
public final class FlightOptions {

    /** Server host name. */
    public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue()
            .withDescription("Arrow Flight SQL server host.");

    /** Server port. */
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().defaultValue(8815)
            .withDescription("Arrow Flight SQL server port.");

    /** User for basic auth (also sent with a bearer token as the user name). */
    public static final ConfigOption<String> USER = ConfigOptions.key("user").stringType().noDefaultValue()
            .withDescription("User name (basic auth, or the user a bearer token belongs to).");

    /** Password for basic auth. */
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue()
            .withDescription("Password (basic auth).");

    /** Bearer token; takes precedence over the password. */
    public static final ConfigOption<String> TOKEN = ConfigOptions.key("token").stringType().noDefaultValue()
            .withDescription("Bearer token; used instead of the password when set.");

    /** Whether to connect with TLS. */
    public static final ConfigOption<Boolean> TLS = ConfigOptions.key("tls").booleanType().defaultValue(false)
            .withDescription("Connect with TLS.");

    /** Whether to verify the server certificate when TLS is on. */
    public static final ConfigOption<Boolean> TLS_VERIFY = ConfigOptions.key("tls.verify").booleanType()
            .defaultValue(true).withDescription("Verify the server certificate (TLS only).");

    /** Truststore with the certificate the server's certificate must chain to (TLS only). */
    public static final ConfigOption<String> TLS_TRUSTSTORE = ConfigOptions.key("tls.truststore").stringType()
            .noDefaultValue().withDescription("Path of a truststore (JKS or PKCS12) holding the certificate to "
                    + "trust instead of the JVM's default trust anchors (TLS with verification only).");

    /** Password of the truststore. */
    public static final ConfigOption<String> TLS_TRUSTSTORE_PASSWORD = ConfigOptions.key("tls.truststore.password")
            .stringType().defaultValue("").withDescription("Password of the truststore ('tls.truststore').");

    /** Default schema for unqualified table names. */
    public static final ConfigOption<String> DEFAULT_SCHEMA = ConfigOptions.key("default-schema").stringType()
            .noDefaultValue().withDescription("Schema for unqualified table names.");

    /** Kerberos service principal (Kerberos auth when set). */
    public static final ConfigOption<String> KERBEROS_PRINCIPAL = ConfigOptions.key("kerberos.service-principal")
            .stringType().noDefaultValue()
            .withDescription("Kerberos service principal of the server; enables Kerberos (SPNEGO) auth.");

    /** Connect timeout. */
    public static final ConfigOption<Duration> CONNECT_TIMEOUT = ConfigOptions.key("connect-timeout")
            .durationType().defaultValue(Duration.ofSeconds(30)).withDescription("Connect timeout.");

    /** Retries of failed calls (planning, endpoint reads with failover). */
    public static final ConfigOption<Integer> MAX_RETRIES = ConfigOptions.key("max-retries").intType()
            .defaultValue(3).withDescription("Retries of a failed call before the query fails.");

    /** Not instantiable: constants and static helpers only. */
    private FlightOptions() {
    }

    /**
     * Options that must be set.
     *
     * @return the required options
     */
    public static Set<ConfigOption<?>> required() {
        return Set.of(HOST);
    }

    /**
     * Options that may be set.
     *
     * @return the optional options
     */
    public static Set<ConfigOption<?>> optional() {
        return Set.of(PORT, USER, PASSWORD, TOKEN, TLS, TLS_VERIFY, TLS_TRUSTSTORE, TLS_TRUSTSTORE_PASSWORD,
                DEFAULT_SCHEMA, KERBEROS_PRINCIPAL, CONNECT_TIMEOUT, MAX_RETRIES);
    }

    /**
     * Builds the client configuration from validated Flink options.
     *
     * @param options table or catalog options
     * @return the client configuration
     * @throws ValidationException if the options are inconsistent
     */
    public static Configuration toClientConfiguration(ReadableConfig options) {
        String host = options.getOptional(HOST)
                .orElseThrow(() -> new ValidationException("Option '" + HOST.key() + "' is required."));
        int port = options.get(PORT);
        if (port <= 0 || port > 65535) {
            throw new ValidationException("Option '" + PORT.key() + "' must be in 1..65535, got " + port + ".");
        }
        int retries = options.get(MAX_RETRIES);
        if (retries < 0) {
            throw new ValidationException("Option '" + MAX_RETRIES.key() + "' must be >= 0, got " + retries + ".");
        }
        String password = options.getOptional(PASSWORD).orElse(null);
        String token = options.getOptional(TOKEN).orElse(null);
        if (password != null && token != null) {
            throw new ValidationException("Set either '" + PASSWORD.key() + "' or '" + TOKEN.key() + "', not both.");
        }
        String user = options.getOptional(USER).orElse(null);
        boolean tls = options.get(TLS);
        String truststore = options.getOptional(TLS_TRUSTSTORE).orElse(null);
        Configuration config;
        if (truststore == null) {
            config = new Configuration(host, port, tls, options.get(TLS_VERIFY), user, password, token);
        } else {
            if (!tls || !options.get(TLS_VERIFY)) {
                throw new ValidationException("Option '" + TLS_TRUSTSTORE.key() + "' needs '" + TLS.key()
                        + "' = 'true' and '" + TLS_VERIFY.key() + "' = 'true'.");
            }
            config = new Configuration(host, port, truststore, options.get(TLS_TRUSTSTORE_PASSWORD), user,
                    password, token);
            // The client only logs a warning and trusts nothing when the store cannot be read.
            if (config.getCertificateBytes() == null || config.getCertificateBytes().length == 0) {
                throw new ValidationException("No trusted certificate could be read from '" + truststore
                        + "' (option '" + TLS_TRUSTSTORE.key() + "'; check the file and its password).");
            }
        }
        options.getOptional(DEFAULT_SCHEMA).ifPresent(config::setDefaultSchema);
        String principal = options.getOptional(KERBEROS_PRINCIPAL).orElse(null);
        if (principal != null) {
            if (!tls) {
                throw new ValidationException("Option '" + KERBEROS_PRINCIPAL.key() + "' needs '" + TLS.key()
                        + "' = 'true': Kerberos authentication is only offered over TLS.");
            }
            if (password != null || token != null) {
                throw new ValidationException("Set either '" + KERBEROS_PRINCIPAL.key() + "' or a password/token, "
                        + "not both.");
            }
            config.setKerberosServicePrincipal(principal);
        }
        config.setConnectTimeoutMs(options.get(CONNECT_TIMEOUT).toMillis());
        config.setMaxRetries(retries);
        return config;
    }
}
