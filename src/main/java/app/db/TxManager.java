package app.db;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.function.Function;

/** Simple transactional runner using try-with-resources. */
public final class TxManager {
    private final Db db;

    public TxManager(Db db) {
        this.db = db;
    }

    public <T> T withTx(Function<Connection, T> action) {
        try (Connection con = db.getConnection()) {
            boolean prev = con.getAutoCommit();
            con.setAutoCommit(false);
            try {
                T result = action.apply(con);
                con.commit();
                return result;
            } catch (RuntimeException | Error e) {
                try {
                    con.rollback();
                } catch (SQLException ignored) {
                }
                throw e;
            } catch (Exception e) {
                try {
                    con.rollback();
                } catch (SQLException ignored) {
                }
                throw new RuntimeException(e);
            } finally {
                try {
                    con.setAutoCommit(prev);
                } catch (SQLException ignored) {
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
