// src/main/java/app/db/JdbcTemplate.java
package main.java.app.db;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class JdbcTemplate {
    private JdbcTemplate() {
    }

    public static int executeUpdate(Connection con, String sql, Object... params) throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            bind(ps, params);
            return ps.executeUpdate();
        }
    }

    public static <T> List<T> query(Connection con, String sql, RowMapper<T> mapper, Object... params)
            throws SQLException {
        try (PreparedStatement ps = con.prepareStatement(sql)) {
            bind(ps, params);
            try (ResultSet rs = ps.executeQuery()) {
                List<T> list = new ArrayList<>();
                while (rs.next())
                    list.add(mapper.map(rs));
                return list;
            }
        }
    }

    public interface RowMapper<T> {
        T map(ResultSet rs) throws SQLException;
    }

    private static void bind(PreparedStatement ps, Object... params) throws SQLException {
        for (int i = 0; i < params.length; i++)
            ps.setObject(i + 1, params[i]);
    }
}