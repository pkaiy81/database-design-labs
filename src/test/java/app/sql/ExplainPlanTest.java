package app.sql;

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

import static org.junit.jupiter.api.Assertions.*;

class ExplainPlanTest {

    private static final int BLOCK_SIZE = 4096;
    private static final String TABLE = "t_ord";
    private static final String INDEX = "ix_t_ord_id";

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
                .addInt("v"));
        mdm.createIndex(INDEX, TABLE, "id");
        planner = new Planner(fm, mdm);

        insertRow(1, 100);
        insertRow(2, 150);
        insertRow(3, 210);
        insertRow(4, 260);
    }

    @Test
    void explainOrderByLimitUsesIndexOrderScan() {
        Ast.Statement stmt = new Parser("EXPLAIN SELECT id, v FROM t_ord ORDER BY id LIMIT 2").parseStatement();
        Ast.ExplainStmt explain = assertInstanceOf(Ast.ExplainStmt.class, stmt);
        String plan = planner.explain(explain);
        assertEquals(String.join(System.lineSeparator(),
                "Project(cols=id,v)",
                "└─ IndexOrderScan(table=t_ord,index=" + INDEX + ",order=ASC,limit=2)"),
                plan);
    }

    @Test
    void explainEqPredicateCapturesRange() {
        Ast.Statement stmt = new Parser("EXPLAIN SELECT id, v FROM t_ord WHERE id = 1 ORDER BY id").parseStatement();
        Ast.ExplainStmt explain = assertInstanceOf(Ast.ExplainStmt.class, stmt);
        String plan = planner.explain(explain);
        assertEquals(String.join(System.lineSeparator(),
                "Project(cols=id,v)",
                "└─ IndexOrderScan(table=t_ord,index=" + INDEX + ",order=ASC)",
                "   └─ Range(lo=1,hi=1)"),
                plan);
    }

    @Test
    void explainBetweenPredicateShowsRange() {
        Ast.Statement stmt = new Parser("EXPLAIN SELECT id, v FROM t_ord WHERE id BETWEEN 1 AND 2 ORDER BY id")
                .parseStatement();
        Ast.ExplainStmt explain = assertInstanceOf(Ast.ExplainStmt.class, stmt);
        String plan = planner.explain(explain);
        assertEquals(String.join(System.lineSeparator(),
                "Project(cols=id,v)",
                "└─ IndexOrderScan(table=t_ord,index=" + INDEX + ",order=ASC)",
                "   └─ Range(lo=1,hi=2)"),
                plan);
    }

    @Test
    void explainResidualPredicateAppearsAsFilter() {
        Ast.Statement stmt = new Parser(
                "EXPLAIN SELECT id, v FROM t_ord WHERE id >= 1 AND v >= 200 ORDER BY id").parseStatement();
        Ast.ExplainStmt explain = assertInstanceOf(Ast.ExplainStmt.class, stmt);
        String plan = planner.explain(explain);
        assertEquals(String.join(System.lineSeparator(),
                "Project(cols=id,v)",
                "└─ IndexOrderScan(table=t_ord,index=" + INDEX + ",order=ASC)",
                "   └─ Filter(pred=v>=200)",
                "      └─ Range(lo=1,hi=+)"),
                plan);
    }

    private void insertRow(int id, int v) {
        Layout layout = mdm.getLayout(TABLE);
        TableFile tf = new TableFile(fm, TABLE + ".tbl", layout);
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, TABLE);
            ts.beforeFirst();
            ts.insert();
            ts.setInt("id", id);
            ts.setInt("v", v);
        }
    }
}
