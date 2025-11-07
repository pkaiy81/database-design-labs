package app.index.btree;

import app.index.Index;
import app.index.RID;
import app.index.RangeCursor;
import app.index.SearchKey;
import app.storage.BlockId;
import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BTreeIndexBasicTest {

    TestIO.Env env;
    FileMgr fm;
    String dataFile;
    String indexFile;

    @BeforeEach
    void setUp() throws Exception {
        env = new TestIO.Env();
        fm = env.fm;
        dataFile = env.dataFile("t");
        indexFile = env.indexFile("t_c");

        // データファイルを 2 ブロックほど作っておく（RID 用）
        // FileMgr.append(...) を呼べばブロックが増える前提
        fm.append(dataFile); // block#0
        fm.append(dataFile); // block#1
    }

    @AfterEach
    void tearDown() throws Exception {
        env.close();
    }

    private RID rid(int blockNo, int slot) {
        return new RID(new BlockId(dataFile, blockNo), slot);
    }

    private BTreeIndex newIndex() throws Exception {
        return new BTreeIndex(fm, indexFile, dataFile);
    }

    @Test
    void pointLookup_singleAndMultiple() throws Exception {
        try (Index idx = newIndex()) {
            idx.open();

            // key=10 に 2件、key=42 に 1件
            idx.insert(SearchKey.ofInt(10), rid(1, 3));
            idx.insert(SearchKey.ofInt(10), rid(1, 7));
            idx.insert(SearchKey.ofInt(42), rid(0, 2));

            // 既存 key
            idx.beforeFirst(SearchKey.ofInt(10));
            List<RID> got = new ArrayList<>();
            while (idx.next())
                got.add(idx.getDataRid());
            assertEquals(2, got.size());
            assertTrue(got.contains(rid(1, 3)));
            assertTrue(got.contains(rid(1, 7)));

            // 存在しない key
            idx.beforeFirst(SearchKey.ofInt(11));
            assertFalse(idx.next());

            // 単一件
            idx.beforeFirst(SearchKey.ofInt(42));
            assertTrue(idx.next());
            assertEquals(rid(0, 2), idx.getDataRid());
            assertFalse(idx.next());

            idx.close();
        }
    }

    @Test
    void rangeScan_inclusiveExclusive() throws Exception {
        try (BTreeIndex idx = newIndex()) {
            idx.open();
            // 0..9 を block=0/slot=i で投入
            for (int k = 0; k < 10; k++) {
                idx.insert(SearchKey.ofInt(k), rid(0, k));
            }

            // [3,7) = 3,4,5,6
            try (RangeCursor cur = idx.range(SearchKey.ofInt(3), true, SearchKey.ofInt(7), false)) {
                List<Integer> keys = new ArrayList<>();
                List<RID> rids = new ArrayList<>();
                while (cur.next()) {
                    rids.add(cur.getDataRid());
                }
                // 取り出した RID の slot をキーとして再現（今回の投入法に依存）
                for (RID r : rids)
                    keys.add(r.slot());
                assertEquals(List.of(3, 4, 5, 6), keys);
            }

            // (-∞, 2] = 0,1,2
            try (RangeCursor cur = idx.range(null, false, SearchKey.ofInt(2), true)) {
                List<RID> r = new ArrayList<>();
                while (cur.next())
                    r.add(cur.getDataRid());
                assertEquals(3, r.size());
                assertEquals(0, r.get(0).slot());
                assertEquals(1, r.get(1).slot());
                assertEquals(2, r.get(2).slot());
            }

            // [8, +∞) = 8,9
            try (RangeCursor cur = idx.range(SearchKey.ofInt(8), true, null, false)) {
                List<RID> r = new ArrayList<>();
                while (cur.next())
                    r.add(cur.getDataRid());
                assertEquals(2, r.size());
                assertEquals(8, r.get(0).slot());
                assertEquals(9, r.get(1).slot());
            }
        }
    }

    @Test
    void insert_causesSplitsAndRootGrowth() throws Exception {
        try (BTreeIndex idx = newIndex()) {
            idx.open();
            // ブロックサイズ4096/固定スロットなので、ある程度入れると葉→内部の分割が起きる。
            // ここでは 2000 件程度を投入しても数ms〜数十msのはず。
            for (int k = 0; k < 2000; k++) {
                idx.insert(SearchKey.ofInt(k), rid(1, k % 1000));
            }
            // いくつか spot check
            idx.beforeFirst(SearchKey.ofInt(0));
            assertTrue(idx.next());
            assertEquals(0, idx.getDataRid().slot());

            idx.beforeFirst(SearchKey.ofInt(1234));
            assertTrue(idx.next());
            assertEquals(234, idx.getDataRid().slot()); // 1234 % 1000

            // 存在しないキー
            idx.beforeFirst(SearchKey.ofInt(2001));
            assertFalse(idx.next());
        }
    }

    @Test
    void delete_minimalLeafOnly() throws Exception {
        try (BTreeIndex idx = newIndex()) {
            idx.open();
            // key=10 に 2件
            RID r1 = rid(1, 3);
            RID r2 = rid(1, 7);
            idx.insert(SearchKey.ofInt(10), r1);
            idx.insert(SearchKey.ofInt(10), r2);

            // 1件削除
            idx.delete(SearchKey.ofInt(10), r1);
            idx.beforeFirst(SearchKey.ofInt(10));
            assertTrue(idx.next());
            assertEquals(r2, idx.getDataRid());
            assertFalse(idx.next());

            // もう1件も削除 → 空
            idx.delete(SearchKey.ofInt(10), r2);
            idx.beforeFirst(SearchKey.ofInt(10));
            assertFalse(idx.next());
        }
    }

    @Test
    void smoke_insert_and_read_same_leaf() throws Exception {
        try (BTreeIndex idx = newIndex()) {
            idx.open();
            // 分割しない程度の少数だけ
            idx.insert(SearchKey.ofInt(10), rid(1, 3));
            idx.insert(SearchKey.ofInt(10), rid(1, 7));

            // すぐ同じプロセスで point lookup
            idx.beforeFirst(SearchKey.ofInt(10));
            assertTrue(idx.next());
            RID r = idx.getDataRid();
            assertNotNull(r);
        }
    }

}
