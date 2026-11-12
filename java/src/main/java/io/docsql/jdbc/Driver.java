package io.docsql.jdbc;

import io.docsql.DocsqlConnection;
import io.docsql.DocsqlException;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

/** JDBC driver entry point. */
public class Driver implements java.sql.Driver {

    private static final Driver INSTANCE = new Driver();

    static {
        try {
            java.sql.DriverManager.registerDriver(INSTANCE);
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public java.util.logging.Logger getParentLogger() {
        return java.util.logging.Logger.getLogger("io.docsql");
    }

    static Driver driverInstance() {
        return INSTANCE;
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        io.docsql.ConnectConfig cfg = DriverUrl.parse(url, info);
        if (cfg == null) {
            return null;
        }
        try {
            DocsqlConnection core = new DocsqlConnection(cfg);
            return new JdbcConnection(core);
        } catch (DocsqlException e) {
            throw JdbcSql.wrap(e);
        }
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.toLowerCase(java.util.Locale.ROOT).startsWith("jdbc:docsql:");
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
        java.util.List<DriverPropertyInfo> out = new java.util.ArrayList<>();
        for (String name : new String[] {"token", "user", "password", "tls", "tlsCa",
                    "tlsHostname", "key", "timeout", "autocommit"}) {
            DriverPropertyInfo dpi = new DriverPropertyInfo(name, info == null ? null
                    : info.getProperty(name));
            out.add(dpi);
        }
        return out.toArray(new DriverPropertyInfo[0]);
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 9;
    }

    @Override
    public boolean jdbcCompliant() {
        return true;
    }
}
