package app.jdbc;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * MiniDBDriver: JDBC Driver implementation for MiniDB
 * 
 * <p>URL format: jdbc:minidb:<db_directory_path>
 * <p>Example: jdbc:minidb:./mydata
 * 
 * <p>Usage:
 * <pre>{@code
 * Class.forName("app.jdbc.MiniDBDriver");
 * Connection conn = DriverManager.getConnection("jdbc:minidb:./data");
 * }</pre>
 * 
 * <p>This driver is automatically registered with DriverManager via static initializer.
 */
public class MiniDBDriver implements Driver {
    public static final String URL_PREFIX = "jdbc:minidb:";
    private static final int MAJOR_VERSION = 0;
    private static final int MINOR_VERSION = 21;
    
    // 静的初期化子でDriverManagerに自動登録
    static {
        try {
            DriverManager.registerDriver(new MiniDBDriver());
        } catch (SQLException e) {
            throw new RuntimeException("Failed to register MiniDB JDBC driver", e);
        }
    }
    
    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null; // このドライバーでは処理できないURL
        }
        
        try {
            String dbPath = extractDbPath(url);
            return new MiniDBConnection(dbPath);
        } catch (Exception e) {
            throw new SQLException("Failed to connect to MiniDB: " + url, e);
        }
    }
    
    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return url != null && url.startsWith(URL_PREFIX);
    }
    
    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        // 現時点ではプロパティなし
        return new DriverPropertyInfo[0];
    }
    
    @Override
    public int getMajorVersion() {
        return MAJOR_VERSION;
    }
    
    @Override
    public int getMinorVersion() {
        return MINOR_VERSION;
    }
    
    @Override
    public boolean jdbcCompliant() {
        // 完全なJDBC準拠ではない（教育目的の実装）
        return false;
    }
    
    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("MiniDB does not support java.util.logging");
    }
    
    /**
     * URLからデータベースディレクトリパスを抽出
     * 
     * @param url JDBC URL (jdbc:minidb:<path>)
     * @return データベースディレクトリパス
     */
    private String extractDbPath(String url) {
        if (!url.startsWith(URL_PREFIX)) {
            throw new IllegalArgumentException("Invalid MiniDB URL: " + url);
        }
        
        String path = url.substring(URL_PREFIX.length());
        if (path.isEmpty()) {
            throw new IllegalArgumentException("Database path is empty in URL: " + url);
        }
        
        return path;
    }
    
    @Override
    public String toString() {
        return "MiniDB JDBC Driver";
    }
}
