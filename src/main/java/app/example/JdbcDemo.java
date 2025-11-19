package app.example;

import java.sql.*;

/**
 * JDBC Demo: Demonstrates MiniDB JDBC driver usage
 * 
 * <p>This demo shows:
 * <ul>
 *   <li>JDBC Driver loading and connection establishment</li>
 *   <li>DDL operations (CREATE TABLE, CREATE INDEX)</li>
 *   <li>DML operations (INSERT, UPDATE, DELETE, SELECT)</li>
 *   <li>PreparedStatement with parameter binding</li>
 *   <li>Transaction management (commit, rollback)</li>
 *   <li>ResultSet iteration and metadata</li>
 * </ul>
 */
public class JdbcDemo {
    private static final String DB_URL = "jdbc:minidb:data-demo";
    
    public static void main(String[] args) {
        System.out.println("=== MiniDB JDBC Driver Demo ===\n");
        
        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            System.out.println("✓ Connected to database: " + DB_URL);
            
            // Display database info
            DatabaseMetaData meta = conn.getMetaData();
            System.out.println("  Database: " + meta.getDatabaseProductName() + 
                             " " + meta.getDatabaseProductVersion());
            System.out.println("  Driver: " + meta.getDriverName() + 
                             " " + meta.getDriverVersion());
            System.out.println("  JDBC: " + meta.getJDBCMajorVersion() + 
                             "." + meta.getJDBCMinorVersion());
            System.out.println();
            
            // Scenario 1: DDL Operations
            scenario1_DDL(conn);
            
            // Scenario 2: Basic DML
            scenario2_BasicDML(conn);
            
            // Scenario 3: PreparedStatement
            scenario3_PreparedStatement(conn);
            
            // Scenario 4: Transaction Management
            scenario4_Transactions(conn);
            
            // Scenario 5: ResultSet Metadata
            scenario5_Metadata(conn);
            
            System.out.println("\n=== Demo completed successfully ===");
            
        } catch (SQLException e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    /**
     * Scenario 1: DDL Operations
     */
    private static void scenario1_DDL(Connection conn) throws SQLException {
        System.out.println("--- Scenario 1: DDL Operations ---");
        
        try (Statement stmt = conn.createStatement()) {
            // Create table
            stmt.executeUpdate("CREATE TABLE products (id INT, name VARCHAR(50), price INT)");
            System.out.println("✓ Created table: products");
            
            // Create index
            stmt.executeUpdate("CREATE INDEX idx_product_name ON products (name)");
            System.out.println("✓ Created index: idx_product_name");
        }
        
        System.out.println();
    }
    
    /**
     * Scenario 2: Basic DML Operations
     */
    private static void scenario2_BasicDML(Connection conn) throws SQLException {
        System.out.println("--- Scenario 2: Basic DML Operations ---");
        
        try (Statement stmt = conn.createStatement()) {
            // Insert
            int inserted = stmt.executeUpdate(
                "INSERT INTO products (id, name, price) VALUES (1, 'Laptop', 1200)"
            );
            System.out.println("✓ Inserted " + inserted + " row(s)");
            
            stmt.executeUpdate(
                "INSERT INTO products (id, name, price) VALUES (2, 'Mouse', 25)"
            );
            stmt.executeUpdate(
                "INSERT INTO products (id, name, price) VALUES (3, 'Keyboard', 75)"
            );
            System.out.println("✓ Inserted 2 more rows");
            
            // Update
            int updated = stmt.executeUpdate(
                "UPDATE products SET price = 1100 WHERE id = 1"
            );
            System.out.println("✓ Updated " + updated + " row(s): Laptop price reduced");
            
            // Select
            ResultSet rs = stmt.executeQuery("SELECT id, name, price FROM products");
            System.out.println("\n  Current products:");
            System.out.println("  ID | Name         | Price");
            System.out.println("  ---|--------------|------");
            while (rs.next()) {
                System.out.printf("  %-2d | %-12s | $%d%n", 
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("price"));
            }
            rs.close();
            
            // Delete
            int deleted = stmt.executeUpdate("DELETE FROM products WHERE id = 2");
            System.out.println("\n✓ Deleted " + deleted + " row(s): Mouse removed");
        }
        
        System.out.println();
    }
    
    /**
     * Scenario 3: PreparedStatement with Parameters
     */
    private static void scenario3_PreparedStatement(Connection conn) throws SQLException {
        System.out.println("--- Scenario 3: PreparedStatement ---");
        
        // Insert with parameters
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO products (id, name, price) VALUES (?, ?, ?)"
        )) {
            pstmt.setInt(1, 4);
            pstmt.setString(2, "Monitor");
            pstmt.setInt(3, 300);
            pstmt.executeUpdate();
            System.out.println("✓ Inserted Monitor using PreparedStatement");
        }
        
        // Query with parameter
        try (PreparedStatement pstmt = conn.prepareStatement(
            "SELECT id, name, price FROM products WHERE price > ?"
        )) {
            pstmt.setInt(1, 100);
            ResultSet rs = pstmt.executeQuery();
            
            System.out.println("\n  Products with price > $100:");
            System.out.println("  ID | Name         | Price");
            System.out.println("  ---|--------------|------");
            while (rs.next()) {
                System.out.printf("  %-2d | %-12s | $%d%n", 
                    rs.getInt("id"),
                    rs.getString("name"),
                    rs.getInt("price"));
            }
            rs.close();
        }
        
        System.out.println();
    }
    
    /**
     * Scenario 4: Transaction Management
     */
    private static void scenario4_Transactions(Connection conn) throws SQLException {
        System.out.println("--- Scenario 4: Transaction Management ---");
        
        // Test commit
        conn.setAutoCommit(false);
        System.out.println("✓ Auto-commit disabled");
        
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                "INSERT INTO products (id, name, price) VALUES (5, 'Tablet', 500)"
            );
            System.out.println("  Inserted Tablet (not committed)");
            
            conn.commit();
            System.out.println("✓ Transaction committed: Tablet saved");
        }
        
        // Test rollback
        try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                "INSERT INTO products (id, name, price) VALUES (6, 'Headphones', 80)"
            );
            System.out.println("  Inserted Headphones (not committed)");
            
            conn.rollback();
            System.out.println("✓ Transaction rolled back: Headphones discarded");
        }
        
        conn.setAutoCommit(true);
        System.out.println("✓ Auto-commit re-enabled");
        
        // Verify final state
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) as cnt FROM products");
            rs.next();
            int count = rs.getInt("cnt");
            System.out.println("  Final product count: " + count + " (Tablet saved, Headphones rolled back)");
            rs.close();
        }
        
        System.out.println();
    }
    
    /**
     * Scenario 5: ResultSet Metadata
     */
    private static void scenario5_Metadata(Connection conn) throws SQLException {
        System.out.println("--- Scenario 5: ResultSet Metadata ---");
        
        try (Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id, name, price FROM products");
            ResultSetMetaData meta = rs.getMetaData();
            
            System.out.println("  ResultSet metadata:");
            System.out.println("  Column Count: " + meta.getColumnCount());
            
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                System.out.printf("  Column %d: %s (%s)%n",
                    i,
                    meta.getColumnName(i),
                    meta.getColumnTypeName(i));
            }
            
            rs.close();
        }
        
        System.out.println();
    }
}
