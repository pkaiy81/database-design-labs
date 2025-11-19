package app.jdbc;

import app.query.Scan;
import app.sql.Ast;
import app.sql.Parser;
import app.sql.Planner;
import app.tx.Tx;

import java.sql.*;

/**
 * MiniDBStatement: JDBC Statement implementation for MiniDB
 * 
 * <p>Executes SQL statements and returns results:
 * <ul>
 *   <li>executeQuery(): SELECT statements → ResultSet</li>
 *   <li>executeUpdate(): INSERT/UPDATE/DELETE/DDL → affected row count</li>
 *   <li>execute(): Any SQL → boolean (true if ResultSet, false if update count)</li>
 * </ul>
 */
public class MiniDBStatement implements Statement {
    protected final MiniDBConnection connection;
    private boolean closed = false;
    private ResultSet currentResultSet = null;
    private int updateCount = -1;
    
    public MiniDBStatement(MiniDBConnection connection) {
        this.connection = connection;
    }
    
    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        closeCurrentResultSet();
        
        try {
            Planner planner = connection.getPlanner();
            Tx tx = connection.getCurrentTransaction();
            
            Parser parser = new Parser(sql);
            Ast.Statement stmt = parser.parseStatement();
            
            if (!(stmt instanceof Ast.SelectStmt selectStmt)) {
                throw new SQLException("executeQuery() requires a SELECT statement: " + sql);
            }
            
            Scan scan = planner.plan(sql);
            
            // Build schema from SELECT statement
            app.record.Schema schema = buildSchemaFromSelect(selectStmt);
            
            currentResultSet = new MiniDBResultSet(this, scan, tx, schema);
            updateCount = -1;
            
            // Auto-commit after query if autoCommit is on
            if (connection.getAutoCommit()) {
                // ResultSet will handle commit when closed
            }
            
            return currentResultSet;
        } catch (Exception e) {
            if (connection.getAutoCommit() && connection.getCurrentTransaction() != null) {
                connection.rollback();
                connection.getCurrentTransaction();  // Start new transaction
            }
            throw new SQLException("Failed to execute query: " + sql, e);
        }
    }
    
    /**
     * Build schema from SELECT statement by examining table layout
     */
    private app.record.Schema buildSchemaFromSelect(Ast.SelectStmt selectStmt) {
        // Get base table layout
        app.metadata.MetadataManager mdm = connection.getMetadataManager();
        app.record.Layout layout = mdm.getLayout(selectStmt.from.table);
        app.record.Schema baseSchema = layout.schema();
        
        // For simplicity, return the base table schema
        // A full implementation would handle:
        // - SELECT field list filtering
        // - JOIN column merging
        // - Aggregate function result columns
        return baseSchema;
    }
    
    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        closeCurrentResultSet();
        
        try {
            Planner planner = connection.getPlanner();
            Tx tx = connection.getCurrentTransaction();
            
            Parser parser = new Parser(sql);
            Ast.Statement stmt = parser.parseStatement();
            
            int count = 0;
            
            if (stmt instanceof Ast.CreateTableStmt createStmt) {
                planner.executeCreateTable(createStmt);
                count = 0;
            } else if (stmt instanceof Ast.DropTableStmt dropStmt) {
                planner.executeDropTable(dropStmt);
                count = 0;
            } else if (stmt instanceof Ast.CreateIndexStmt createIdxStmt) {
                // CREATE INDEX is not yet implemented in Planner
                throw new SQLException("CREATE INDEX not yet supported via JDBC");
            } else if (stmt instanceof Ast.CreateIndexStmt) {
                throw new SQLException("CREATE INDEX not yet supported via JDBC");
            } else if (stmt instanceof Ast.DropIndexStmt dropIdxStmt) {
                planner.executeDropIndex(dropIdxStmt);
                count = 0;
            } else if (stmt instanceof Ast.InsertStmt insertStmt) {
                count = planner.executeInsert(insertStmt, tx);
            } else if (stmt instanceof Ast.UpdateStmt updateStmt) {
                count = planner.executeUpdate(updateStmt, tx);
            } else if (stmt instanceof Ast.DeleteStmt deleteStmt) {
                count = planner.executeDelete(deleteStmt, tx);
            } else if (stmt instanceof Ast.SelectStmt) {
                throw new SQLException("executeUpdate() cannot be used with SELECT statements. Use executeQuery() instead.");
            } else {
                throw new SQLException("Unsupported statement type: " + stmt.getClass().getSimpleName());
            }
            
            updateCount = count;
            currentResultSet = null;
            
            // Auto-commit after update if autoCommit is on
            if (connection.getAutoCommit()) {
                tx.commit();
                connection.afterAutoCommit(); // End transaction so new one starts on next operation
            }
            
            return count;
        } catch (Exception e) {
            if (connection.getAutoCommit()) {
                Tx tx = connection.getCurrentTransaction();
                if (tx != null) {
                    tx.rollback();
                }
            }
            throw new SQLException("Failed to execute update: " + sql, e);
        }
    }
    
    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        
        // Parse to determine statement type
        Parser parser = new Parser(sql);
        Ast.Statement stmt = parser.parseStatement();
        
        if (stmt instanceof Ast.SelectStmt) {
            executeQuery(sql);
            return true; // ResultSet available
        } else {
            executeUpdate(sql);
            return false; // Update count available
        }
    }
    
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        
        closeCurrentResultSet();
        closed = true;
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return currentResultSet;
    }
    
    @Override
    public int getUpdateCount() throws SQLException {
        checkClosed();
        return updateCount;
    }
    
    @Override
    public boolean getMoreResults() throws SQLException {
        checkClosed();
        closeCurrentResultSet();
        return false; // MiniDB doesn't support multiple result sets
    }
    
    @Override
    public Connection getConnection() throws SQLException {
        checkClosed();
        return connection;
    }
    
    /**
     * Closes the current ResultSet if any
     */
    private void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null && !currentResultSet.isClosed()) {
            currentResultSet.close();
        }
        currentResultSet = null;
    }
    
    /**
     * Checks if statement is closed and throws SQLException if it is
     */
    protected void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
    }
    
    // ========== Unsupported operations ==========
    
    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }
    
    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }
    
    @Override
    public void setMaxRows(int max) throws SQLException {
        // Ignore
    }
    
    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }
    
    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        // Ignore
    }
    
    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("cancel not supported");
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
        // No warnings
    }
    
    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Named cursors not supported");
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
        // Ignore
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }
    
    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }
    
    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }
    
    @Override
    public void clearBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }
    
    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }
    
    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return getMoreResults();
    }
    
    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys not supported");
    }
    
    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }
    
    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }
    
    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        // Ignore
    }
    
    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }
    
    @Override
    public void closeOnCompletion() throws SQLException {
        // Ignore
    }
    
    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
