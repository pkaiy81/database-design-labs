package app.demo;

import java.sql.*;

/**
 * JDBC Driver Demo - Shows various JDBC operations with MiniDB
 * 
 * This demo covers:
 * 1. Basic CRUD operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
 * 2. PreparedStatement with parameter binding
 * 3. Transaction control (commit/rollback)
 * 4. Metadata queries
 * 5. String escaping (SQL standard)
 * 
 * Usage:
 *   java app.demo.JdbcDemo
 * 
 * Note: This demo uses jdbc:minidb:data-jdbc-demo as the database directory.
 *       All data will be stored in ./data-jdbc-demo/
 */
public class JdbcDemo {
    private static final String JDBC_URL = "jdbc:minidb:data-jdbc-demo";
    
    public static void main(String[] args) {
        System.out.println("=".repeat(70));
        System.out.println("MiniDB JDBC Driver Demo");
        System.out.println("=".repeat(70));
        System.out.println();
        
        try {
            // Load JDBC driver (auto-registered via META-INF/services)
            Class.forName("app.jdbc.MiniDBDriver");
            System.out.println("✓ JDBC Driver loaded: MiniDB JDBC Driver");
            System.out.println();
            
            // Run all demo scenarios
            demo1_BasicCRUD();
            System.out.println();
            
            demo2_PreparedStatement();
            System.out.println();
            
            demo3_TransactionCommit();
            System.out.println();
            
            demo4_MetadataQueries();
            System.out.println();
            
            demo5_StringEscaping();
            System.out.println();
            
            System.out.println("=".repeat(70));
            System.out.println("All demos completed successfully!");
            System.out.println("=".repeat(70));
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Demo 1: Basic CRUD Operations
     * - CREATE TABLE
     * - INSERT
     * - SELECT
     * - UPDATE
     * - DELETE
     * - DROP TABLE
     */
    private static void demo1_BasicCRUD() throws SQLException {
        System.out.println("Demo 1: Basic CRUD Operations");
        System.out.println("-".repeat(70));
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            // CREATE TABLE
            stmt.executeUpdate("CREATE TABLE employees (id INT, name STRING(50), salary INT)");
            System.out.println("✓ Table created: employees");
            
            // INSERT
            stmt.executeUpdate("INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 50000)");
            stmt.executeUpdate("INSERT INTO employees (id, name, salary) VALUES (2, 'Bob', 60000)");
            stmt.executeUpdate("INSERT INTO employees (id, name, salary) VALUES (3, 'Charlie', 55000)");
            System.out.println("✓ Inserted 3 rows");
            
            // SELECT
            System.out.println("\n  SELECT * FROM employees:");
            ResultSet rs = stmt.executeQuery("SELECT id, name, salary FROM employees");
            printResultSet(rs);
            rs.close();
            
            // UPDATE
            stmt.executeUpdate("UPDATE employees SET salary = 65000 WHERE id = 2");
            System.out.println("✓ Updated Bob's salary to 65000");
            
            System.out.println("\n  After UPDATE:");
            rs = stmt.executeQuery("SELECT id, name, salary FROM employees WHERE id = 2");
            printResultSet(rs);
            rs.close();
            
            // DELETE
            stmt.executeUpdate("DELETE FROM employees WHERE id = 3");
            System.out.println("✓ Deleted Charlie (id=3)");
            
            System.out.println("\n  After DELETE:");
            rs = stmt.executeQuery("SELECT id, name, salary FROM employees");
            printResultSet(rs);
            rs.close();
            
            // DROP TABLE
            stmt.executeUpdate("DROP TABLE employees");
            System.out.println("✓ Table dropped: employees");
        }
    }
    
    /**
     * Demo 2: PreparedStatement with Parameter Binding
     * - Demonstrates ? placeholders
     * - Automatic parameter escaping
     * - Reusable SQL statements
     */
    private static void demo2_PreparedStatement() throws SQLException {
        System.out.println("Demo 2: PreparedStatement with Parameter Binding");
        System.out.println("-".repeat(70));
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            // CREATE TABLE
            stmt.executeUpdate("CREATE TABLE products (id INT, name STRING(50), price INT)");
            System.out.println("✓ Table created: products");
            
            // PreparedStatement INSERT
            String insertSQL = "INSERT INTO products (id, name, price) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                // Insert product 1
                pstmt.setInt(1, 101);
                pstmt.setString(2, "Laptop");
                pstmt.setInt(3, 1200);
                pstmt.executeUpdate();
                
                // Insert product 2
                pstmt.setInt(1, 102);
                pstmt.setString(2, "Mouse");
                pstmt.setInt(3, 25);
                pstmt.executeUpdate();
                
                // Insert product 3
                pstmt.setInt(1, 103);
                pstmt.setString(2, "Keyboard");
                pstmt.setInt(3, 75);
                pstmt.executeUpdate();
                
                System.out.println("✓ Inserted 3 products using PreparedStatement");
            }
            
            // PreparedStatement SELECT
            System.out.println("\n  SELECT with WHERE clause (price < ?):");
            String selectSQL = "SELECT id, name, price FROM products WHERE price < ?";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 100);
                ResultSet rs = pstmt.executeQuery();
                printResultSet(rs);
                rs.close();
            }
            
            // Cleanup
            stmt.executeUpdate("DROP TABLE products");
            System.out.println("✓ Table dropped: products");
        }
    }
    
    /**
     * Demo 3: Transaction Control
     * - AutoCommit = false
     * - Multiple operations in one transaction
     * - Commit
     */
    private static void demo3_TransactionCommit() throws SQLException {
        System.out.println("Demo 3: Transaction Control (Commit)");
        System.out.println("-".repeat(70));
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            // CREATE TABLE
            stmt.executeUpdate("CREATE TABLE accounts (id INT, balance INT)");
            stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
            stmt.executeUpdate("INSERT INTO accounts (id, balance) VALUES (2, 500)");
            System.out.println("✓ Table created: accounts");
            System.out.println("  Initial state:");
            ResultSet rs = stmt.executeQuery("SELECT id, balance FROM accounts");
            printResultSet(rs);
            rs.close();
            
            // Start transaction
            conn.setAutoCommit(false);
            System.out.println("\n✓ AutoCommit disabled - Transaction started");
            
            // Transfer 200 from account 1 to account 2
            stmt.executeUpdate("UPDATE accounts SET balance = 800 WHERE id = 1");
            stmt.executeUpdate("UPDATE accounts SET balance = 700 WHERE id = 2");
            System.out.println("  Executed 2 updates (transfer 200 from acc1 to acc2)");
            
            // Commit
            conn.commit();
            System.out.println("✓ Transaction committed");
            
            // Verify
            conn.setAutoCommit(true);
            System.out.println("\n  After commit:");
            rs = stmt.executeQuery("SELECT id, balance FROM accounts");
            printResultSet(rs);
            rs.close();
            
            // Cleanup
            stmt.executeUpdate("DROP TABLE accounts");
            System.out.println("✓ Table dropped: accounts");
        }
    }
    
    /**
     * Demo 4: Metadata Queries
     * - DatabaseMetaData
     * - getTables()
     * - getColumns()
     * - Database product information
     */
    private static void demo4_MetadataQueries() throws SQLException {
        System.out.println("Demo 4: Metadata Queries");
        System.out.println("-".repeat(70));
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            // Create sample tables
            stmt.executeUpdate("CREATE TABLE customers (id INT, name STRING(50))");
            stmt.executeUpdate("CREATE TABLE orders (order_id INT, customer_id INT, amount INT)");
            System.out.println("✓ Created tables: customers, orders");
            
            // Get DatabaseMetaData
            DatabaseMetaData meta = conn.getMetaData();
            
            // Database info
            System.out.println("\nDatabase Information:");
            System.out.println("  Product Name: " + meta.getDatabaseProductName());
            System.out.println("  Product Version: " + meta.getDatabaseProductVersion());
            System.out.println("  JDBC Driver: " + meta.getDriverName());
            System.out.println("  JDBC Version: " + meta.getJDBCMajorVersion() + "." + meta.getJDBCMinorVersion());
            
            // Note: getTables() and getColumns() are implemented but require
            // column mapping from physical catalog columns to JDBC metadata columns.
            // This is left as a future enhancement.
            System.out.println("\nMetadata queries (getTables/getColumns) are available");
            System.out.println("but require column mapping for full functionality.");
            
            // Cleanup
            stmt.executeUpdate("DROP TABLE customers");
            stmt.executeUpdate("DROP TABLE orders");
            System.out.println("\n✓ Tables dropped: customers, orders");
        }
    }
    
    /**
     * Demo 5: String Escaping (SQL Standard)
     * - Single quotes in strings: 'O''Brien'
     * - PreparedStatement auto-escaping
     */
    private static void demo5_StringEscaping() throws SQLException {
        System.out.println("Demo 5: String Escaping (SQL Standard)");
        System.out.println("-".repeat(70));
        
        try (Connection conn = DriverManager.getConnection(JDBC_URL);
             Statement stmt = conn.createStatement()) {
            
            // CREATE TABLE
            stmt.executeUpdate("CREATE TABLE people (id INT, name STRING(50))");
            System.out.println("✓ Table created: people");
            
            // PreparedStatement with single quote in string
            String insertSQL = "INSERT INTO people (id, name) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "O'Brien"); // Contains single quote
                pstmt.executeUpdate();
                System.out.println("✓ Inserted: O'Brien (PreparedStatement auto-escaped)");
                
                pstmt.setInt(1, 2);
                pstmt.setString(2, "It's working!");
                pstmt.executeUpdate();
                System.out.println("✓ Inserted: It's working!");
                
                pstmt.setInt(1, 3);
                pstmt.setString(2, "Quote '' test");
                pstmt.executeUpdate();
                System.out.println("✓ Inserted: Quote '' test");
            }
            
            // SELECT and verify
            System.out.println("\n  SELECT * FROM people:");
            ResultSet rs = stmt.executeQuery("SELECT id, name FROM people");
            printResultSet(rs);
            rs.close();
            
            // Cleanup
            stmt.executeUpdate("DROP TABLE people");
            System.out.println("✓ Table dropped: people");
        }
    }
    
    /**
     * Helper: Print ResultSet in table format
     */
    private static void printResultSet(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columnCount = meta.getColumnCount();
        
        // Print column headers
        System.out.print("  | ");
        for (int i = 1; i <= columnCount; i++) {
            System.out.print(meta.getColumnName(i) + " | ");
        }
        System.out.println();
        System.out.println("  " + "-".repeat(60));
        
        // Print rows
        while (rs.next()) {
            System.out.print("  | ");
            for (int i = 1; i <= columnCount; i++) {
                String colName = meta.getColumnName(i);
                Object value = rs.getObject(colName); // Use column name instead of index
                System.out.print(value + " | ");
            }
            System.out.println();
        }
    }
}
