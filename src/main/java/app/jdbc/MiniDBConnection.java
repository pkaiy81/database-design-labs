package app.jdbc;

import app.memory.BufferMgr;
import app.memory.LogManager;
import app.metadata.MetadataManager;
import app.sql.Planner;
import app.storage.FileMgr;
import app.tx.Tx;
import app.tx.lock.IsolationLevel;

import java.io.File;
import java.nio.file.Paths;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

/**
 * MiniDBConnection: JDBC Connection implementation for MiniDB
 * 
 * <p>Manages database connection lifecycle:
 * <ul>
 *   <li>Transaction management (commit, rollback, autoCommit)</li>
 *   <li>Statement creation (Statement, PreparedStatement)</li>
 *   <li>Isolation level control</li>
 *   <li>Database metadata access</li>
 * </ul>
 * 
 * <p>Each connection has its own transaction context.
 */
public class MiniDBConnection implements Connection {
    final String dbPath;
    private final java.nio.file.Path dbDir;
    private final FileMgr fileMgr;
    private final BufferMgr bufferMgr;
    private final LogManager logManager;
    private final MetadataManager metadataManager;
    private final Planner planner;
    
    private Tx currentTx;
    private boolean autoCommit = true;
    private boolean closed = false;
    private IsolationLevel isolationLevel = IsolationLevel.READ_COMMITTED;
    
    /**
     * Creates a new connection to MiniDB
     * 
     * @param dbPath database directory path
     * @throws SQLException if connection fails
     */
    public MiniDBConnection(String dbPath) throws SQLException {
        try {
            this.dbPath = dbPath;
            File dbDirFile = new File(dbPath);
            this.dbDir = dbDirFile.toPath();
            
            // Initialize core components
            this.fileMgr = new FileMgr(this.dbDir, 4096);
            this.bufferMgr = new BufferMgr(fileMgr, 8, 3000);
            this.logManager = new LogManager(this.dbDir);
            
            // Initialize metadata manager
            this.metadataManager = new MetadataManager(fileMgr);
            
            this.planner = new Planner(fileMgr, metadataManager);
            
            // Start initial transaction if not autoCommit
            if (!autoCommit) {
                beginTransaction();
            }
        } catch (Exception e) {
            throw new SQLException("Failed to initialize MiniDB connection: " + dbPath, e);
        }
    }
    
    /**
     * Gets the current transaction, creating one if needed
     */
    Tx getCurrentTransaction() throws SQLException {
        checkClosed();
        if (currentTx == null) {
            beginTransaction();
        }
        return currentTx;
    }
    
    /**
     * Gets the planner instance
     */
    Planner getPlanner() {
        return planner;
    }
    
    /**
     * Gets the FileMgr instance
     */
    FileMgr getFileMgr() {
        return fileMgr;
    }
    
    /**
     * Gets the MetadataManager instance
     */
    MetadataManager getMetadataManager() {
        return metadataManager;
    }
    
    /**
     * Begins a new transaction
     */
    private void beginTransaction() throws SQLException {
        if (currentTx != null) {
            throw new SQLException("Transaction already active");
        }
        currentTx = new Tx(fileMgr, bufferMgr, logManager, dbDir, isolationLevel);
    }
    
    /**
     * Ends the current transaction
     */
    private void endTransaction() {
        if (currentTx != null) {
            currentTx = null;
        }
    }
    
    /**
     * Called after auto-commit to end the transaction
     */
    void afterAutoCommit() {
        endTransaction();
    }
    
    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new MiniDBStatement(this);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        checkClosed();
        return new MiniDBPreparedStatement(this, sql);
    }
    
    @Override
    public void commit() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot commit in autoCommit mode");
        }
        if (currentTx != null) {
            currentTx.commit();
            endTransaction();
        }
    }
    
    @Override
    public void rollback() throws SQLException {
        checkClosed();
        if (autoCommit) {
            throw new SQLException("Cannot rollback in autoCommit mode");
        }
        if (currentTx != null) {
            currentTx.rollback();
            endTransaction();
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        
        try {
            // Rollback any active transaction
            if (currentTx != null) {
                currentTx.rollback();
                endTransaction();
            }
            
            // LogManager has close() method
            if (logManager != null) {
                logManager.close();
            }
        } finally {
            closed = true;
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        checkClosed();
        if (this.autoCommit == autoCommit) {
            return; // No change
        }
        
        // Commit current transaction if switching from manual to auto
        if (autoCommit && currentTx != null) {
            currentTx.commit();
            endTransaction();
        }
        
        this.autoCommit = autoCommit;
        
        // Start new transaction if switching from auto to manual
        if (!autoCommit && currentTx == null) {
            beginTransaction();
        }
    }
    
    @Override
    public boolean getAutoCommit() throws SQLException {
        checkClosed();
        return autoCommit;
    }
    
    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        checkClosed();
        
        // Map JDBC isolation levels to MiniDB IsolationLevel
        this.isolationLevel = switch (level) {
            case Connection.TRANSACTION_READ_UNCOMMITTED -> IsolationLevel.READ_UNCOMMITTED;
            case Connection.TRANSACTION_READ_COMMITTED -> IsolationLevel.READ_COMMITTED;
            case Connection.TRANSACTION_REPEATABLE_READ -> IsolationLevel.REPEATABLE_READ;
            case Connection.TRANSACTION_SERIALIZABLE -> IsolationLevel.SERIALIZABLE;
            default -> throw new SQLException("Unsupported isolation level: " + level);
        };
        
        // If there's an active transaction, restart it with new isolation level
        if (currentTx != null && !autoCommit) {
            currentTx.commit();
            endTransaction();
            beginTransaction();
        }
    }
    
    @Override
    public int getTransactionIsolation() throws SQLException {
        checkClosed();
        return switch (isolationLevel) {
            case READ_UNCOMMITTED -> Connection.TRANSACTION_READ_UNCOMMITTED;
            case READ_COMMITTED -> Connection.TRANSACTION_READ_COMMITTED;
            case REPEATABLE_READ -> Connection.TRANSACTION_REPEATABLE_READ;
            case SERIALIZABLE -> Connection.TRANSACTION_SERIALIZABLE;
        };
    }
    
    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        checkClosed();
        return new MiniDBDatabaseMetaData(this, metadataManager, fileMgr);
    }
    
    @Override
    public boolean isValid(int timeout) throws SQLException {
        return !closed;
    }
    
    /**
     * Checks if connection is closed and throws SQLException if it is
     */
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }
    
    // ========== Unsupported operations ==========
    
    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql; // No transformation needed
    }
    
    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }
    
    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        // Ignore for now
    }
    
    @Override
    public String getCatalog() throws SQLException {
        return dbPath;
    }
    
    @Override
    public void setCatalog(String catalog) throws SQLException {
        throw new SQLFeatureNotSupportedException("setCatalog not supported");
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null; // No warnings
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        // No warnings to clear
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement(); // Ignore parameters
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql); // Ignore parameters
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        throw new SQLFeatureNotSupportedException("Type map not supported");
    }
    
    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        throw new SQLFeatureNotSupportedException("Type map not supported");
    }
    
    @Override
    public void setHoldability(int holdability) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public Savepoint setSavepoint() throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        throw new SQLFeatureNotSupportedException("Savepoints not supported");
    }
    
    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("CallableStatement not supported");
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }
    
    @Override
    public Clob createClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB not supported");
    }
    
    @Override
    public Blob createBlob() throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB not supported");
    }
    
    @Override
    public NClob createNClob() throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB not supported");
    }
    
    @Override
    public SQLXML createSQLXML() throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array not supported");
    }
    
    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Struct not supported");
    }
    
    @Override
    public void setSchema(String schema) throws SQLException {
        // Ignore
    }
    
    @Override
    public String getSchema() throws SQLException {
        return null;
    }
    
    @Override
    public void abort(Executor executor) throws SQLException {
        close();
    }
    
    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }
    
    @Override
    public Properties getClientInfo() throws SQLException {
        return new Properties();
    }
    
    @Override
    public String getClientInfo(String name) throws SQLException {
        return null;
    }
    
    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        // Not supported
    }
    
    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        // Not supported
    }
    
    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    
    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
