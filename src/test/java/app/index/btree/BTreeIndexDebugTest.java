package app.index.btree;

import app.index.RID;
import app.index.SearchKey;
import app.storage.BlockId;
import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class BTreeIndexDebugTest {

    static final int BLOCK_SIZE = 4096;

    static class Env implements AutoCloseable {
        final Path dir;
        final FileMgr fm;
        final String dataFile;
        final String indexFile;

        Env(String base) throws IOException {
            this.dir = Files.createTempDirectory("btree-debug-");
            this.fm = new FileMgr(dir, BLOCK_SIZE);
            this.dataFile = base + ".tbl";
            this.indexFile = "idx_" + base;

            // データファイルに少なくとも2ブロック作成（RID 用）
            fm.append(dataFile); // #0
            fm.append(dataFile); // #1
        }

        @Override
        public void close() throws Exception {
            try (var s = Files.walk(dir)) {
                s.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignored) {
                    }
                });
            }
        }
    }

    private RID rid(String dataFile, int blockNo, int slot) {
        return new RID(new BlockId(dataFile, blockNo), slot);
    }

    // --- ダンプ系ユーティリティ（同一パッケージなので BTPage へ直接アクセス可能） ---

    /**
     * ルート BlockId を返す（BTreeIndex のフィールド root は private なので、descendToLeaf(任意のキー) で
     * root の型を判断しつつ辿る）
     */
    private BlockId getRoot(FileMgr fm, String indexFile) throws Exception {
        // ルートは常に block#0 とは限らない実装もあるが、今回の実装では 0 または append によるブロック。
        // ここでは単純に 0 を「候補」として返す。正確に知りたい場合は BTreeIndex の private フィールド root を反射で見る。
        return new BlockId(indexFile, 0);
    }

    /** BTreeIndex.descendToLeaf(int) を反射で呼び出し、指定キーが属する葉ブロックを取得 */
    private BlockId descendToLeaf(BTreeIndex idx, int key) throws Exception {
        Method m = BTreeIndex.class.getDeclaredMethod("descendToLeaf", int.class);
        m.setAccessible(true);
        return (BlockId) m.invoke(idx, key);
    }

    /** ブロックのヘッダとスロットをすべて表示（内部／葉 どちらでもOK） */
    private void dumpPage(FileMgr fm, BlockId blk) throws Exception {
        try (BTPage p = new BTPage(fm, blk)) {
            System.out.println("== PAGE " + blk.filename() + "#" + blk.number()
                    + " level=" + p.level()
                    + " isLeaf=" + p.isLeaf()
                    + " count=" + p.keyCount()
                    + " prev=" + p.prev()
                    + " next=" + p.next());
            if (p.isLeaf()) {
                for (int i = 0; i < p.keyCount(); i++) {
                    System.out.println("  [L] i=" + i + " key=" + p.leafKey(i)
                            + " rid=(" + p.leafBlockNo(i) + "," + p.leafRidSlot(i) + ")");
                }
            } else {
                for (int i = 0; i < p.keyCount(); i++) {
                    System.out.println("  [D] i=" + i + " minKey=" + p.dirKey(i)
                            + " child=" + p.dirChild(i));
                }
            }
        }
    }

    /** ツリーの「最も左の葉」まで降りてから、next で全葉を順にダンプ */
    private void dumpAllLeaves(FileMgr fm, String indexFile) throws Exception {
        BlockId cur = new BlockId(indexFile, 0);
        // 内部なら最左子を辿る
        while (true) {
            try (BTPage p = new BTPage(fm, cur)) {
                if (p.isLeaf())
                    break;
                int child = p.dirChild(0);
                cur = new BlockId(indexFile, child);
            }
        }
        // 左端の葉から next で右へ
        BlockId leafBlk = cur;
        while (leafBlk != null) {
            dumpPage(fm, leafBlk);
            try (BTPage p = new BTPage(fm, leafBlk)) {
                int nxt = p.next();
                leafBlk = (nxt == -1) ? null : new BlockId(indexFile, nxt);
            }
        }
    }

    /** 指定キーの属する葉をダンプ */
    private void dumpLeafForKey(FileMgr fm, String indexFile, String dataFile, int key) throws Exception {
        try (BTreeIndex idx = new BTreeIndex(fm, indexFile, dataFile)) {
            BlockId leafBlk = descendToLeaf(idx, key);
            System.out.println("-- leaf for key=" + key + " is " + leafBlk.filename() + "#" + leafBlk.number());
            dumpPage(fm, leafBlk);
        }
    }

    // ------------------------- テスト本体 -------------------------

    @Test
    void debug_smallInserts_dumpTree() throws Exception {
        try (Env env = new Env("t")) {
            FileMgr fm = env.fm;
            String dataFile = env.dataFile;
            String indexFile = env.indexFile;

            try (BTreeIndex idx = new BTreeIndex(fm, indexFile, dataFile)) {
                idx.open();

                // 小規模挿入（重複含む）
                idx.insert(SearchKey.ofInt(10), rid(dataFile, 1, 3));
                idx.insert(SearchKey.ofInt(10), rid(dataFile, 1, 7));
                idx.insert(SearchKey.ofInt(5), rid(dataFile, 0, 1));
                idx.insert(SearchKey.ofInt(20), rid(dataFile, 0, 2));
                idx.insert(SearchKey.ofInt(15), rid(dataFile, 1, 9));
            }

            System.out.println("=== dump all leaves after small inserts ===");
            dumpAllLeaves(fm, indexFile);
            System.out.println("=== dump leaf for key=10 ===");
            dumpLeafForKey(fm, indexFile, dataFile, 10);

            // ここでは落とさない（ダンプのためのテスト）
            assertTrue(true);
        }
    }

    @Test
    void debug_smoke_visibility_and_pointLookup() throws Exception {
        try (Env env = new Env("t2")) {
            FileMgr fm = env.fm;
            String dataFile = env.dataFile;
            String indexFile = env.indexFile;

            try (BTreeIndex idx = new BTreeIndex(fm, indexFile, dataFile)) {
                idx.open();
                // smoke と同じ手順
                idx.insert(SearchKey.ofInt(10), rid(dataFile, 1, 3));
                idx.insert(SearchKey.ofInt(10), rid(dataFile, 1, 7));

                // 書いた直後の葉の可視性をダンプ
                System.out.println("=== dump leaf for key=10 (just after insert) ===");
                dumpLeafForKey(fm, indexFile, dataFile, 10);

                // すぐに point lookup
                idx.beforeFirst(SearchKey.ofInt(10));
                boolean has = idx.next();
                System.out.println("next() returned = " + has);
                if (has) {
                    RID r = idx.getDataRid();
                    System.out.println("getDataRid() = (" + r.block().number() + "," + r.slot() + ")");
                } else {
                    System.out.println("getDataRid() skipped because next()==false");
                }
                // ここでは落とさない（まずはダンプ優先）
                assertTrue(true);
            }
        }
    }

    @Test
    void debug_bulk_insert_and_dump_root_and_leaves() throws Exception {
        try (Env env = new Env("t3")) {
            FileMgr fm = env.fm;
            String dataFile = env.dataFile;
            String indexFile = env.indexFile;

            try (BTreeIndex idx = new BTreeIndex(fm, indexFile, dataFile)) {
                idx.open();
                // 連番を挿入して分割を誘発
                IntStream.range(0, 2000).forEach(k -> idx.insert(SearchKey.ofInt(k), rid(dataFile, 1, k % 1000)));
            }
            System.out.println("=== dump all leaves after bulk inserts ===");
            dumpAllLeaves(fm, indexFile);
            // 代表で key=1234 の属する葉もダンプ
            System.out.println("=== dump leaf for key=1234 ===");
            dumpLeafForKey(fm, indexFile, dataFile, 1234);

            assertTrue(true);
        }
    }
}
