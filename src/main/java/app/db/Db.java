package app.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Minimal connection helper. Prefer DataSource, but fall back to DriverManager.
 */
public final class Db {
    private final DataSource dataSource;
    private final String url;
    private final String user;
    private final String pass;

    private Db(DataSource ds, String url, String user, String pass) {
        this.dataSource = ds;
        this.url = url;
        this.user = user;
        this.pass = pass;
    }

    public static Db fromDataSource(DataSource ds) {
        return new Db(Objects.requireNonNull(ds), null, null, null);
    }

    public static Db fromUrl(String url, String user, String pass) {
        return new Db(null, Objects.requireNonNull(url), user, pass);
    }

    public Connection getConnection() throws SQLException {
        if (dataSource != null)
            return dataSource.getConnection();
        if (user == null)
            return DriverManager.getConnection(url);
        return DriverManager.getConnection(url, user, pass);
    }
}
