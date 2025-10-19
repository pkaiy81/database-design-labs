package app.sql;

import app.metadata.MetadataManager;
import app.query.IndexOrderScan;
import app.query.Scan;
import app.query.ProjectScan;
import app.record.Schema;
import app.storage.FileMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.*;

class OrderByIndexOrderScanTest {

    private static final int BLOCK_SIZE = 4096;
    private static final String TABLE = "orders";
    private static final String INDEX = "idx_orders_id";

    @TempDir
    Path tempDir;

    private FileMgr fm;
    private MetadataManager mdm;
    private Planner planner;

    @BeforeEach
    void setUp() {
        fm = new FileMgr(tempDir, BLOCK_SIZE);
        mdm = new MetadataManager(fm);
        mdm.createTable(TABLE, new Schema()
                .addInt("id")
                .addInt("value")
                .addString("note", 20));
        mdm.createIndex(INDEX, TABLE, "id");
        planner = new Planner(fm, mdm);

        insertRow(1, 10, "a");
        insertRow(2, 20, "b");
        insertRow(3, 30, "c");
        insertRow(4, 40, "d");
        insertRow(5, 50, "b");
    }

    @Test
    void orderByOnIndexedColumnUsesIndexOrderScan() {
        List<Row> rows = runQueryExpectingIndexScan("SELECT id, value FROM orders ORDER BY id");
        assertEquals(List.of(
                new Row(1, 10),
                new Row(2, 20),
                new Row(3, 30),
                new Row(4, 40),
                new Row(5, 50)
        ), rows);
    }

    @Test
    void rangePredicateAndLimitStopAfterKRows() {
        List<Row> rows = runQueryExpectingIndexScan(
                "SELECT id, value FROM orders WHERE id >= 2 AND id < 5 ORDER BY id LIMIT 2");
        assertEquals(List.of(
                new Row(2, 20),
                new Row(3, 30)
        ), rows);
    }

    @Test
    void residualPredicatesAreEvaluatedAfterIndexOrder() {
        List<Row> rows = runQueryExpectingIndexScan(
                "SELECT id, value FROM orders WHERE id >= 1 AND value = 20 ORDER BY id");
        assertEquals(List.of(
        new Row(2, 20)
        ), rows);
    }

    private List<Row> runQueryExpectingIndexScan(String sql) {
        Ast.SelectStmt stmt = new Parser(sql).parseSelect();
        Scan rawScan = planner.plan(stmt);
        Scan leaf = unwrapScan(rawScan);
        assertInstanceOf(IndexOrderScan.class, leaf, "Expected planner to choose IndexOrderScan");
        try (Scan scan = rawScan) {
            scan.beforeFirst();
            List<Row> rows = new ArrayList<>();
            while (scan.next()) {
                rows.add(new Row(scan.getInt("id"), scan.getInt("value")));
            }
            return rows;
        }
    }

    private Scan unwrapScan(Scan scan) {
        Scan current = scan;
        while (current instanceof ProjectScan) {
            current = extractInnerScan((ProjectScan) current);
        }
        return current;
    }

    private Scan extractInnerScan(ProjectScan projectScan) {
        try {
            Field field = ProjectScan.class.getDeclaredField("s");
            field.setAccessible(true);
            return (Scan) field.get(projectScan);
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertRow(int id, int value, String note) {
        Ast.InsertStmt insert = parseInsert(
                "INSERT INTO orders(id, value, note) VALUES (" + id + ", " + value + ", '" + note + "')");
        assertEquals(1, planner.executeInsert(insert));
    }

    private Ast.InsertStmt parseInsert(String sql) {
        Ast.Statement stmt = new Parser(sql).parseStatement();
        return assertInstanceOf(Ast.InsertStmt.class, stmt);
    }

    private record Row(int id, int value) {
    }
}
