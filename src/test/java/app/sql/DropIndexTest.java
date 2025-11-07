package app.sql;

import app.index.btree.BTreeIndex;
import app.metadata.MetadataManager;
import app.record.Schema;
import app.storage.FileMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class DropIndexTest {

    private static final int BLOCK_SIZE = 4096;

    @TempDir
    Path tempDir;

    private FileMgr fm;
    private MetadataManager mdm;
    private Planner planner;

    @BeforeEach
    void setUp() {
        fm = new FileMgr(tempDir, BLOCK_SIZE);
        mdm = new MetadataManager(fm);
        mdm.createTable("t_drop", new Schema().addInt("id"));
        mdm.createIndex("ix_t_drop_id", "t_drop", "id");
        try (BTreeIndex ignored = new BTreeIndex(fm, "ix_t_drop_id", "t_drop.tbl")) {
            ignored.open();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        planner = new Planner(fm, mdm);
    }

    @Test
    void dropIndexRemovesCatalogAndFile() {
        Ast.DropIndexStmt drop = new Ast.DropIndexStmt("ix_t_drop_id");
        assertTrue(planner.executeDropIndex(drop));
        assertTrue(mdm.listIndexesFormatted().isEmpty());
        assertFalse(Files.exists(tempDir.resolve("ix_t_drop_id")));
        assertFalse(Files.exists(tempDir.resolve("ix_t_drop_id.idx")));
    }

    @Test
    void droppingMissingIndexReturnsFalse() {
        Ast.DropIndexStmt drop = new Ast.DropIndexStmt("missing_index");
        assertFalse(planner.executeDropIndex(drop));
    }
}
