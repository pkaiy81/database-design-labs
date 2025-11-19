package app.jdbc;

import app.record.Schema;
import app.record.FieldType;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * MiniDBResultSetMetaData: Provides metadata about ResultSet columns
 * 
 * <p>Extracts column information from Schema:
 * <ul>
 *   <li>Column names</li>
 *   <li>Column types (INT or STRING)</li>
 *   <li>Column count</li>
 * </ul>
 */
public class MiniDBResultSetMetaData implements ResultSetMetaData {
    private final List<String> columnNames;
    private final Schema schema;
    
    public MiniDBResultSetMetaData(Schema schema) {
        this.columnNames = new ArrayList<>();
        this.schema = schema;
        
        // Collect all column names from schema
        for (String fieldName : schema.fields().keySet()) {
            columnNames.add(fieldName);
        }
    }
    
    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }
    
    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnNames.get(column - 1); // JDBC is 1-indexed
    }
    
    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }
    
    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        String columnName = columnNames.get(column - 1);
        
        // Check if it's an integer field
        FieldType fieldType = schema.fieldType(columnName);
        if (fieldType == FieldType.INT) {
            return Types.INTEGER;
        } else if (fieldType == FieldType.STRING) {
            return Types.VARCHAR;
        } else {
            throw new SQLException("Unknown field type for column: " + columnName);
        }
    }
    
    @Override
    public String getColumnTypeName(int column) throws SQLException {
        checkColumnIndex(column);
        String columnName = columnNames.get(column - 1);
        
        FieldType fieldType = schema.fieldType(columnName);
        if (fieldType == FieldType.INT) {
            return "INTEGER";
        } else if (fieldType == FieldType.STRING) {
            return "VARCHAR";
        } else {
            throw new SQLException("Unknown field type for column: " + columnName);
        }
    }
    
    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return Integer.class.getName();
            case Types.VARCHAR:
                return String.class.getName();
            default:
                return Object.class.getName();
        }
    }
    
    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return 11; // -2147483648 to 2147483647
            case Types.VARCHAR:
                // VARCHAR display size (maxChars is package-private, use default)
                return 50;
            default:
                return 0;
        }
    }
    
    @Override
    public int getPrecision(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.INTEGER:
                return 10; // 10 digits for 32-bit integer
            case Types.VARCHAR:
                // VARCHAR precision (maxChars is package-private, use default)
                return 50;
            default:
                return 0;
        }
    }
    
    @Override
    public int getScale(int column) throws SQLException {
        int type = getColumnType(column);
        if (type == Types.INTEGER) {
            return 0; // Integers have no decimal places
        }
        return 0;
    }
    
    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false; // MiniDB doesn't support auto-increment
    }
    
    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.VARCHAR; // Strings are case-sensitive
    }
    
    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true; // All columns can be used in WHERE clause
    }
    
    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false; // No currency type
    }
    
    @Override
    public int isNullable(int column) throws SQLException {
        return columnNullableUnknown; // MiniDB doesn't track nullability yet
    }
    
    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.INTEGER; // Only integers are signed
    }
    
    @Override
    public String getSchemaName(int column) throws SQLException {
        return ""; // MiniDB doesn't have schemas
    }
    
    @Override
    public String getTableName(int column) throws SQLException {
        return ""; // We don't track table names in ResultSet
    }
    
    @Override
    public String getCatalogName(int column) throws SQLException {
        return ""; // MiniDB doesn't have catalogs
    }
    
    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true; // ResultSets are read-only
    }
    
    @Override
    public boolean isWritable(int column) throws SQLException {
        return false; // ResultSets are read-only
    }
    
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false; // ResultSets are read-only
    }
    
    /**
     * Validates column index (1-indexed)
     */
    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Invalid column index: " + column + 
                ". Valid range: 1 to " + columnNames.size());
        }
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
