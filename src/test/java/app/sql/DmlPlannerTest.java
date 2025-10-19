package app.sql;

import app.index.RangeCursor;
import app.index.RID;
import app.index.btree.BTreeIndex;
import app.metadata.MetadataManager;
import app.record.Layout;
import app.record.Schema;
import app.record.TableFile;
import app.record.TableScan;
import app.storage.FileMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DmlPlannerTest {

    private static final int BLOCK_SIZE = 4096;
    private static final String PEOPLE_TABLE = "people";
    private static final String PEOPLE_INDEX = "idx_people_id";

    @TempDir
    Path tempDir;

    private FileMgr fm;
    private MetadataManager mdm;
    private Planner planner;

    @BeforeEach
    void setUp() {
        fm = new FileMgr(tempDir, BLOCK_SIZE);
        mdm = new MetadataManager(fm);
        mdm.createTable(PEOPLE_TABLE, new Schema()
                .addInt("id")
                .addString("name", 20));
        mdm.createIndex(PEOPLE_INDEX, PEOPLE_TABLE, "id");
        planner = new Planner(fm, mdm);
    }

    @Test
    void insertStatementInsertsRowAndUpdatesIndex() {
        Ast.InsertStmt stmt = parseInsert("INSERT INTO people(id, name) VALUES (1, 'Ada')");
        int rows = planner.executeInsert(stmt);
        assertEquals(1, rows);

        assertEquals(List.of(new PersonRow(1, "Ada")), readAllRows());
        assertEquals(List.of(1), readIdsViaIndex());
    }

    @Test
    void updateStatementUpdatesRowsAndRewritesIndexEntries() {
        insertRow(1, "Ada");

        Ast.UpdateStmt stmt = parseUpdate("UPDATE people SET id = 2, name = 'Alan' WHERE id = 1");
        int rows = planner.executeUpdate(stmt);
        assertEquals(1, rows);

        assertEquals(List.of(new PersonRow(2, "Alan")), readAllRows());
        assertEquals(List.of(2), readIdsViaIndex());
    }

    @Test
    void deleteStatementRemovesRowsAndIndexEntries() {
        insertRow(1, "Ada");
        insertRow(2, "Alan");

        Ast.DeleteStmt stmt = parseDelete("DELETE FROM people WHERE id = 1");
        int rows = planner.executeDelete(stmt);
        assertEquals(1, rows);

        assertEquals(List.of(new PersonRow(2, "Alan")), readAllRows());
        assertEquals(List.of(2), readIdsViaIndex());
    }

    private Ast.InsertStmt parseInsert(String sql) {
        Ast.Statement stmt = new Parser(sql).parseStatement();
        return assertInstanceOf(Ast.InsertStmt.class, stmt);
    }

    private Ast.UpdateStmt parseUpdate(String sql) {
        Ast.Statement stmt = new Parser(sql).parseStatement();
        return assertInstanceOf(Ast.UpdateStmt.class, stmt);
    }

    private Ast.DeleteStmt parseDelete(String sql) {
        Ast.Statement stmt = new Parser(sql).parseStatement();
        return assertInstanceOf(Ast.DeleteStmt.class, stmt);
    }

    private void insertRow(int id, String name) {
        Ast.InsertStmt insert = parseInsert("INSERT INTO people(id, name) VALUES (" + id + ", '" + name + "')");
        assertEquals(1, planner.executeInsert(insert));
    }

    private List<PersonRow> readAllRows() {
        Layout layout = mdm.getLayout(PEOPLE_TABLE);
        TableFile tf = new TableFile(fm, PEOPLE_TABLE + ".tbl", layout);
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.beforeFirst();
            List<PersonRow> rows = new ArrayList<>();
            while (ts.next()) {
                rows.add(new PersonRow(ts.getInt("id"), ts.getString("name")));
            }
            rows.sort(Comparator.comparingInt(PersonRow::id));
            return rows;
        }
    }

    private List<Integer> readIdsViaIndex() {
        Layout layout = mdm.getLayout(PEOPLE_TABLE);
        TableFile tf = new TableFile(fm, PEOPLE_TABLE + ".tbl", layout);
        try (TableScan ts = new TableScan(fm, tf);
             BTreeIndex idx = new BTreeIndex(fm, PEOPLE_INDEX, PEOPLE_TABLE + ".tbl")) {
            ts.beforeFirst();
            idx.open();
            List<Integer> ids = new ArrayList<>();
            try (RangeCursor cursor = idx.range(null, true, null, true)) {
                while (cursor.next()) {
                    RID rid = cursor.getDataRid();
                    try {
                        if (!ts.moveTo(rid))
                            throw new IllegalStateException("RID not found: " + rid);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    ids.add(ts.getInt("id"));
                }
            }
            ids.sort(Integer::compareTo);
            return ids;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private record PersonRow(int id, String name) {
    }
}
