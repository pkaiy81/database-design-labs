package app.jdbc;

import app.record.Schema;
import app.record.TableScan;
import app.tx.Tx;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.Map;

/**
 * MiniDBMetadataResultSet: Special ResultSet wrapper for metadata queries
 * 
 * <p>Wraps a TableScan for system catalogs (tblcat, fldcat, idxcat) 
 * and provides JDBC ResultSet interface.
 */
public class MiniDBMetadataResultSet implements ResultSet {
    private final TableScan scan;
    private final Schema schema;
    private final Tx transaction;
    private boolean closed = false;
    private boolean beforeFirst = true;
    private boolean afterLast = false;
    
    public MiniDBMetadataResultSet(TableScan scan, Schema schema, Tx transaction) {
        this.scan = scan;
        this.schema = schema;
        this.transaction = transaction;
        scan.beforeFirst();
    }
    
    @Override
    public boolean next() throws SQLException {
        checkClosed();
        
        if (afterLast) {
            return false;
        }
        
        if (beforeFirst) {
            beforeFirst = false;
        }
        
        boolean hasNext = scan.next();
        if (!hasNext) {
            afterLast = true;
        }
        
        return hasNext;
    }
    
    @Override
    public void close() throws SQLException {
        if (closed) {
            return;
        }
        
        try {
            scan.close();
        } finally {
            closed = true;
        }
    }
    
    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }
    
    @Override
    public int getInt(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Use getInt(String columnLabel) instead");
    }
    
    @Override
    public int getInt(String columnLabel) throws SQLException {
        checkClosed();
        checkPosition();
        try {
            return scan.getInt(columnLabel);
        } catch (Exception e) {
            throw new SQLException("Failed to get int value for column: " + columnLabel, e);
        }
    }
    
    @Override
    public String getString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Use getString(String columnLabel) instead");
    }
    
    @Override
    public String getString(String columnLabel) throws SQLException {
        checkClosed();
        checkPosition();
        try {
            return scan.getString(columnLabel);
        } catch (Exception e) {
            throw new SQLException("Failed to get string value for column: " + columnLabel, e);
        }
    }
    
    @Override
    public boolean wasNull() throws SQLException {
        return false;
    }
    
    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        checkClosed();
        return new MiniDBResultSetMetaData(schema);
    }
    
    @Override
    public Object getObject(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException("Use getObject(String columnLabel) instead");
    }
    
    @Override
    public Object getObject(String columnLabel) throws SQLException {
        checkClosed();
        checkPosition();
        
        try {
            return scan.getInt(columnLabel);
        } catch (Exception e1) {
            try {
                return scan.getString(columnLabel);
            } catch (Exception e2) {
                throw new SQLException("Failed to get object for column: " + columnLabel);
            }
        }
    }
    
    @Override
    public int findColumn(String columnLabel) throws SQLException {
        throw new SQLFeatureNotSupportedException("findColumn not supported");
    }
    
    @Override
    public Statement getStatement() throws SQLException {
        return null; // Metadata ResultSets don't have associated statements
    }
    
    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        return beforeFirst;
    }
    
    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        return afterLast;
    }
    
    @Override
    public boolean isFirst() throws SQLException {
        return !beforeFirst && !afterLast;
    }
    
    @Override
    public boolean isLast() throws SQLException {
        return false;
    }
    
    @Override
    public int getRow() throws SQLException {
        return 0;
    }
    
    @Override
    public int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }
    
    @Override
    public int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }
    
    @Override
    public int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }
    
    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }
    
    @Override
    public void clearWarnings() throws SQLException {
    }
    
    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }
    
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException("Only FETCH_FORWARD supported");
        }
    }
    
    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }
    
    @Override
    public void setFetchSize(int rows) throws SQLException {
    }
    
    private void checkClosed() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }
    
    private void checkPosition() throws SQLException {
        if (beforeFirst) {
            throw new SQLException("ResultSet is before first row. Call next() first.");
        }
        if (afterLast) {
            throw new SQLException("ResultSet is after last row.");
        }
    }
    
    // ========== All other methods - same as MiniDBResultSet ==========
    
    @Override public boolean previous() throws SQLException { 
        throw new SQLFeatureNotSupportedException("Backward scrolling not supported"); 
    }
    @Override public void beforeFirst() throws SQLException { 
        throw new SQLFeatureNotSupportedException("beforeFirst not supported"); 
    }
    @Override public void afterLast() throws SQLException { 
        throw new SQLFeatureNotSupportedException("afterLast not supported"); 
    }
    @Override public boolean first() throws SQLException { 
        throw new SQLFeatureNotSupportedException("first not supported"); 
    }
    @Override public boolean last() throws SQLException { 
        throw new SQLFeatureNotSupportedException("last not supported"); 
    }
    @Override public boolean absolute(int row) throws SQLException { 
        throw new SQLFeatureNotSupportedException("absolute not supported"); 
    }
    @Override public boolean relative(int rows) throws SQLException { 
        throw new SQLFeatureNotSupportedException("relative not supported"); 
    }
    @Override public boolean getBoolean(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BOOLEAN not supported"); 
    }
    @Override public boolean getBoolean(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BOOLEAN not supported"); 
    }
    @Override public byte getByte(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BYTE not supported"); 
    }
    @Override public byte getByte(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BYTE not supported"); 
    }
    @Override public short getShort(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("SHORT not supported"); 
    }
    @Override public short getShort(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("SHORT not supported"); 
    }
    @Override public long getLong(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("LONG not supported"); 
    }
    @Override public long getLong(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("LONG not supported"); 
    }
    @Override public float getFloat(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("FLOAT not supported"); 
    }
    @Override public float getFloat(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("FLOAT not supported"); 
    }
    @Override public double getDouble(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DOUBLE not supported"); 
    }
    @Override public double getDouble(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DOUBLE not supported"); 
    }
    @Override public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BigDecimal not supported"); 
    }
    @Override public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BigDecimal not supported"); 
    }
    @Override public BigDecimal getBigDecimal(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BigDecimal not supported"); 
    }
    @Override public BigDecimal getBigDecimal(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BigDecimal not supported"); 
    }
    @Override public byte[] getBytes(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BYTES not supported"); 
    }
    @Override public byte[] getBytes(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BYTES not supported"); 
    }
    @Override public Date getDate(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DATE not supported"); 
    }
    @Override public Date getDate(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DATE not supported"); 
    }
    @Override public Time getTime(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIME not supported"); 
    }
    @Override public Time getTime(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIME not supported"); 
    }
    @Override public Timestamp getTimestamp(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported"); 
    }
    @Override public Timestamp getTimestamp(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported"); 
    }
    @Override public InputStream getAsciiStream(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public InputStream getAsciiStream(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public InputStream getUnicodeStream(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public InputStream getUnicodeStream(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public InputStream getBinaryStream(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public InputStream getBinaryStream(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("InputStream not supported"); 
    }
    @Override public String getCursorName() throws SQLException { 
        throw new SQLFeatureNotSupportedException("Named cursors not supported"); 
    }
    @Override public Reader getCharacterStream(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Reader not supported"); 
    }
    @Override public Reader getCharacterStream(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Reader not supported"); 
    }
    @Override public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Type map not supported"); 
    }
    @Override public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Type map not supported"); 
    }
    @Override public Ref getRef(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("REF not supported"); 
    }
    @Override public Ref getRef(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("REF not supported"); 
    }
    @Override public Blob getBlob(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BLOB not supported"); 
    }
    @Override public Blob getBlob(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("BLOB not supported"); 
    }
    @Override public Clob getClob(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("CLOB not supported"); 
    }
    @Override public Clob getClob(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("CLOB not supported"); 
    }
    @Override public Array getArray(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ARRAY not supported"); 
    }
    @Override public Array getArray(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ARRAY not supported"); 
    }
    @Override public Date getDate(int columnIndex, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DATE not supported"); 
    }
    @Override public Date getDate(String columnLabel, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("DATE not supported"); 
    }
    @Override public Time getTime(int columnIndex, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIME not supported"); 
    }
    @Override public Time getTime(String columnLabel, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIME not supported"); 
    }
    @Override public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported"); 
    }
    @Override public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException { 
        throw new SQLFeatureNotSupportedException("TIMESTAMP not supported"); 
    }
    @Override public URL getURL(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("URL not supported"); 
    }
    @Override public URL getURL(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("URL not supported"); 
    }
    @Override public boolean rowUpdated() throws SQLException { return false; }
    @Override public boolean rowInserted() throws SQLException { return false; }
    @Override public boolean rowDeleted() throws SQLException { return false; }
    @Override public void updateNull(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBoolean(int columnIndex, boolean x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateByte(int columnIndex, byte x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateShort(int columnIndex, short x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateInt(int columnIndex, int x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateLong(int columnIndex, long x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateFloat(int columnIndex, float x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateDouble(int columnIndex, double x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateString(int columnIndex, String x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBytes(int columnIndex, byte[] x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateDate(int columnIndex, Date x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateTime(int columnIndex, Time x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateObject(int columnIndex, Object x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNull(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBoolean(String columnLabel, boolean x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateByte(String columnLabel, byte x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateShort(String columnLabel, short x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateInt(String columnLabel, int x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateLong(String columnLabel, long x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateFloat(String columnLabel, float x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateDouble(String columnLabel, double x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateString(String columnLabel, String x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBytes(String columnLabel, byte[] x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateDate(String columnLabel, Date x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateTime(String columnLabel, Time x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateObject(String columnLabel, Object x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void insertRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void deleteRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void refreshRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("refreshRow not supported"); 
    }
    @Override public void cancelRowUpdates() throws SQLException { }
    @Override public void moveToInsertRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void moveToCurrentRow() throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateRef(int columnIndex, Ref x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateRef(String columnLabel, Ref x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(int columnIndex, Blob x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(String columnLabel, Blob x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(int columnIndex, Clob x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(String columnLabel, Clob x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateArray(int columnIndex, Array x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateArray(String columnLabel, Array x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public RowId getRowId(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("RowId not supported"); 
    }
    @Override public RowId getRowId(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("RowId not supported"); 
    }
    @Override public void updateRowId(int columnIndex, RowId x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateRowId(String columnLabel, RowId x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNString(int columnIndex, String nString) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNString(String columnLabel, String nString) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(String columnLabel, NClob nClob) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public NClob getNClob(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("NCLOB not supported"); 
    }
    @Override public NClob getNClob(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("NCLOB not supported"); 
    }
    @Override public SQLXML getSQLXML(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("SQLXML not supported"); 
    }
    @Override public SQLXML getSQLXML(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("SQLXML not supported"); 
    }
    @Override public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public String getNString(int columnIndex) throws SQLException { return getString(columnIndex); }
    @Override public String getNString(String columnLabel) throws SQLException { return getString(columnLabel); }
    @Override public Reader getNCharacterStream(int columnIndex) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Reader not supported"); 
    }
    @Override public Reader getNCharacterStream(String columnLabel) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Reader not supported"); 
    }
    @Override public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(int columnIndex, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(String columnLabel, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(int columnIndex, Reader x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(int columnIndex, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateClob(String columnLabel, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(int columnIndex, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public void updateNClob(String columnLabel, Reader reader) throws SQLException { 
        throw new SQLFeatureNotSupportedException("ResultSet is read-only"); 
    }
    @Override public <T> T getObject(int columnIndex, Class<T> type) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Type-specific getObject not supported"); 
    }
    @Override public <T> T getObject(String columnLabel, Class<T> type) throws SQLException { 
        throw new SQLFeatureNotSupportedException("Type-specific getObject not supported"); 
    }
    @Override public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) return iface.cast(this);
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }
    @Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }
}
