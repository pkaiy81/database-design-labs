package app.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * MiniDBPreparedStatement: JDBC PreparedStatement implementation for MiniDB
 * 
 * <p>Supports parameter binding for SQL statements with placeholders (?)
 * <p>Currently supports: setInt(), setString(), setNull()
 * 
 * <p>Example:
 * <pre>{@code
 * PreparedStatement ps = conn.prepareStatement("SELECT * FROM users WHERE age > ?");
 * ps.setInt(1, 25);
 * ResultSet rs = ps.executeQuery();
 * }</pre>
 */
public class MiniDBPreparedStatement extends MiniDBStatement implements PreparedStatement {
    private final String sqlTemplate;
    private final Map<Integer, Object> parameters = new HashMap<>();
    
    public MiniDBPreparedStatement(MiniDBConnection connection, String sql) {
        super(connection);
        this.sqlTemplate = sql;
    }
    
    @Override
    public ResultSet executeQuery() throws SQLException {
        return executeQuery(buildSql());
    }
    
    @Override
    public int executeUpdate() throws SQLException {
        return executeUpdate(buildSql());
    }
    
    @Override
    public boolean execute() throws SQLException {
        return execute(buildSql());
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, null);
    }
    
    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        checkClosed();
        parameters.put(parameterIndex, x);
    }
    
    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        parameters.clear();
    }
    
    /**
     * Builds the final SQL by replacing ? placeholders with parameter values
     */
    private String buildSql() throws SQLException {
        String sql = sqlTemplate;
        int paramIndex = 1;
        int pos = 0;
        
        while ((pos = sql.indexOf('?', pos)) != -1) {
            if (!parameters.containsKey(paramIndex)) {
                throw new SQLException("Parameter " + paramIndex + " not set");
            }
            
            Object value = parameters.get(paramIndex);
            String replacement;
            
            if (value == null) {
                replacement = "NULL";
            } else if (value instanceof String) {
                // Escape single quotes in strings
                String str = (String) value;
                str = str.replace("'", "''");
                replacement = "'" + str + "'";
            } else {
                replacement = value.toString();
            }
            
            sql = sql.substring(0, pos) + replacement + sql.substring(pos + 1);
            pos += replacement.length();
            paramIndex++;
        }
        
        return sql;
    }
    
    // ========== Unsupported setXXX methods ==========
    
    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setInt(parameterIndex, x ? 1 : 0);
    }
    
    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setInt(parameterIndex, x);
    }
    
    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setInt(parameterIndex, x);
    }
    
    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        throw new SQLFeatureNotSupportedException("LONG not supported");
    }
    
    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        throw new SQLFeatureNotSupportedException("FLOAT not supported");
    }
    
    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        throw new SQLFeatureNotSupportedException("DOUBLE not supported");
    }
    
    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BigDecimal not supported");
    }
    
    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BYTES not supported");
    }
    
    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException("DATE not supported");
    }
    
    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException("TIME not supported");
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported");
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        if (x == null) {
            setNull(parameterIndex, targetSqlType);
        } else if (x instanceof Integer) {
            setInt(parameterIndex, (Integer) x);
        } else if (x instanceof String) {
            setString(parameterIndex, (String) x);
        } else {
            throw new SQLFeatureNotSupportedException("Unsupported object type: " + x.getClass());
        }
    }
    
    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setObject(parameterIndex, x, Types.OTHER);
    }
    
    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader not supported");
    }
    
    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("REF not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB not supported");
    }
    
    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("ARRAY not supported");
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("getMetaData not supported on PreparedStatement");
    }
    
    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("DATE not supported");
    }
    
    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("TIME not supported");
    }
    
    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported");
    }
    
    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }
    
    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        throw new SQLFeatureNotSupportedException("URL not supported");
    }
    
    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("ParameterMetaData not supported");
    }
    
    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId not supported");
    }
    
    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB not supported");
    }
    
    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }
    
    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x, targetSqlType);
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader not supported");
    }
    
    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("InputStream not supported");
    }
    
    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Reader not supported");
    }
    
    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCharacterStream not supported");
    }
    
    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("CLOB not supported");
    }
    
    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("BLOB not supported");
    }
    
    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("NCLOB not supported");
    }
}
