package sparrow.flink;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import org.apache.arrow.driver.jdbc.ArrowFlightJdbcDriver;

/**
 * The Arrow Flight SQL JDBC driver with statement parameters written into the SQL text: the server rejects
 * bound parameters ("put the values into the SQL text"), and Flink's JDBC connector binds every pushed-down
 * filter value and partition bound. A {@link PreparedStatement} here records its parameters and runs the SQL
 * with the values as literals through a plain {@link Statement}. Only what Flink's JDBC source calls is
 * supported; test code for the {@link TpchPerfTest} baseline.
 */
public final class InliningFlightSqlDriver implements Driver {

    private final Driver driver = new ArrowFlightJdbcDriver();

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        Connection connection = driver.connect(url, info);
        if (connection == null) {
            return null;
        }
        return (Connection) Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("prepareStatement".equals(method.getName())) {
                        return prepared(connection, (String) args[0]);
                    }
                    return invoke(connection, method, args);
                });
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return driver.acceptsURL(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return driver.getPropertyInfo(url, info);
    }

    @Override
    public int getMajorVersion() {
        return driver.getMajorVersion();
    }

    @Override
    public int getMinorVersion() {
        return driver.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return driver.getParentLogger();
    }

    private static PreparedStatement prepared(Connection connection, String sql) {
        InvocationHandler handler = new InvocationHandler() {
            private final Map<Integer, Object> parameters = new HashMap<>();
            private int fetchSize;
            private Statement current;

            @Override
            public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                String name = method.getName();
                switch (name) {
                    case "setFetchSize":
                        fetchSize = (Integer) args[0];
                        return null;
                    case "clearParameters":
                        parameters.clear();
                        return null;
                    case "setNull":
                        parameters.put((Integer) args[0], null);
                        return null;
                    case "executeQuery":
                        closeCurrent();
                        current = connection.createStatement();
                        current.setFetchSize(fetchSize);
                        return current.executeQuery(inline(sql, parameters));
                    case "close":
                        closeCurrent();
                        return null;
                    case "isClosed":
                        return false;
                    default:
                        if (name.startsWith("set") && args != null && args.length >= 2
                                && args[0] instanceof Integer index) {
                            parameters.put(index, args[1]);
                            return null;
                        }
                        throw new SQLFeatureNotSupportedException("PreparedStatement." + name);
                }
            }

            private void closeCurrent() throws SQLException {
                if (current != null) {
                    current.close();
                    current = null;
                }
            }
        };
        return (PreparedStatement) Proxy.newProxyInstance(InliningFlightSqlDriver.class.getClassLoader(),
                new Class<?>[] {PreparedStatement.class}, handler);
    }

    /** {@code sql} with its {@code ?} placeholders (outside quotes) replaced by the parameters as literals. */
    static String inline(String sql, Map<Integer, Object> parameters) throws SQLException {
        StringBuilder out = new StringBuilder(sql.length() + 16 * parameters.size());
        char quote = 0;
        int index = 0;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (quote != 0) {
                if (c == quote) {
                    quote = 0;
                }
                out.append(c);
            } else if (c == '\'' || c == '"') {
                quote = c;
                out.append(c);
            } else if (c == '?') {
                index++;
                if (!parameters.containsKey(index)) {
                    throw new SQLException("parameter " + index + " not set: " + sql);
                }
                out.append(literal(parameters.get(index)));
            } else {
                out.append(c);
            }
        }
        if (index != parameters.size()) {
            throw new SQLException(parameters.size() + " parameters for " + index + " placeholders: " + sql);
        }
        return out.toString();
    }

    /** A DuckDB SQL literal of a JDBC parameter value. */
    static String literal(Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        if (value instanceof BigDecimal d) {
            return d.toPlainString();
        }
        if (value instanceof Double d) {
            return "CAST(" + d + " AS DOUBLE)";
        }
        if (value instanceof Float f) {
            return "CAST(" + f + " AS FLOAT)";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Date d) {
            return "DATE '" + d + "'";
        }
        if (value instanceof Time t) {
            return "TIME '" + t + "'";
        }
        if (value instanceof Timestamp t) {
            return "TIMESTAMP '" + t + "'";
        }
        throw new SQLException("no literal for a " + value.getClass().getName());
    }

    private static Object invoke(Object target, Method method, Object[] args) throws Throwable {
        try {
            return method.invoke(target, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
