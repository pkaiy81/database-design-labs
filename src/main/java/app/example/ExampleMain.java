package app.example;

import app.db.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.List;

public class ExampleMain {
    public static void main(String[] args) {
        // Use in-memory H2 for a quick smoke test
        Db db = Db.fromUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1", "sa", "");
        TxManager tx = new TxManager(db);

        tx.withTx((Connection con) -> {
            try {
                JdbcTemplate.executeUpdate(con,
                        "CREATE TABLE IF NOT EXISTS students(id INT PRIMARY KEY, name VARCHAR(64))");
                JdbcTemplate.executeUpdate(con,
                        "INSERT INTO students(id,name) VALUES(?,?)", 1, "Ada");
                List<String> names = JdbcTemplate.query(con,
                        "SELECT name FROM students", (ResultSet rs) -> rs.getString(1));
                names.forEach(System.out::println);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
}
