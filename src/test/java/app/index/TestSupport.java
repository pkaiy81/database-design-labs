package app.index;

import app.storage.BlockId;
import app.storage.FileMgr;

import app.index.btree.BTreeIndex;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntConsumer;

/**
 * 反射なし・非チェック例外化したテスト用ヘルパ。
 */
public class TestSupport {

    @TempDir
    Path tempDir;

    public static final int BLOCK_SIZE = 4096;

    public FileMgr newFileMgr() {
        return new FileMgr(tempDir, BLOCK_SIZE);
    }

    public String dataFileName(String base) {
        return base + ".tbl";
    }

    public String indexFileName(String base) {
        return base + ".idx";
    }

    public RID ridFor(String dataFile, int slot) {
        return new RID(new BlockId(dataFile, 0), slot);
    }

    // ====== カーソルIF（closeはチェック例外を投げない） ======
    public interface EqCursor extends AutoCloseable {
        boolean next();

        RID rid();

        @Override
        default void close() {
        }
    }

    public interface RangeCur extends AutoCloseable {
        boolean next();

        int key();

        RID rid();

        @Override
        default void close() {
        }
    }

    // ====== B+木アダプタ（全部 非チェック） ======
    public interface BTreeIndexAdapter extends AutoCloseable {
        void insert(int key, RID rid);

        void delete(int key, RID rid);

        EqCursor search(int key); // 等値は range(k,k) で代替

        RangeCur range(int lo, int hi); // inclusive

        @Override
        default void close() {
        }
    }

    public BTreeIndexAdapter newIndex(FileMgr fm, String dataFile, String indexBase) {
        return new BTreeIndexAdapter() {
            final BTreeIndex idx;
            {
                try {
                    idx = new BTreeIndex(fm, indexBase, dataFile);
                    idx.open();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void insert(int key, RID rid) {
                try {
                    idx.insert(SearchKey.ofInt(key), rid);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void delete(int key, RID rid) {
                try {
                    idx.delete(SearchKey.ofInt(key), rid);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public EqCursor search(int key) {
                try {
                    final RangeCursor rc = idx.range(
                            SearchKey.ofInt(key), true,
                            SearchKey.ofInt(key), true);
                    return new EqCursor() {
                        @Override
                        public boolean next() {
                            return rc.next();
                        }

                        @Override
                        public RID rid() {
                            return rc.getDataRid();
                        }

                        @Override
                        public void close() {
                            rc.close();
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public RangeCur range(int lo, int hi) {
                try {
                    final RangeCursor rc = idx.range(
                            SearchKey.ofInt(lo), true,
                            SearchKey.ofInt(hi), true);
                    return new RangeCur() {
                        @Override
                        public boolean next() {
                            return rc.next();
                        }

                        @Override
                        public int key() {
                            return rc.getDataRid().slot(); // key は rid.slot に格納している想定
                        }

                        @Override
                        public RID rid() {
                            return rc.getDataRid();
                        }

                        @Override
                        public void close() {
                            rc.close();
                        }
                    };
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void close() {
                try {
                    idx.close();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    // ====== ユーティリティ（これらも非チェック化） ======
    public void insertRange(BTreeIndexAdapter idx, String dataFile, int startInclusive, int endExclusive) {
        for (int i = startInclusive; i < endExclusive; i++) {
            idx.insert(i, ridFor(dataFile, i));
        }
    }

    public void assertOddRemainEvenGone(BTreeIndexAdapter idx, int start, int end) {
        for (int i = start; i < end; i++) {
            try (EqCursor c = idx.search(i)) {
                boolean exists = c.next();
                if ((i & 1) == 0) {
                    if (exists)
                        throw new AssertionError("even " + i + " should be gone");
                } else {
                    if (!exists)
                        throw new AssertionError("odd " + i + " should remain");
                }
            }
        }
    }

    public List<Integer> collectRangeKeys(BTreeIndexAdapter idx, int lo, int hi) {
        List<Integer> out = new ArrayList<>();
        try (RangeCur rc = idx.range(lo, hi)) {
            while (rc.next())
                out.add(rc.key());
        }
        return out;
    }

    public void assertRangeKeys(BTreeIndexAdapter idx, int lo, int hi, IntConsumer checker) {
        try (RangeCur rc = idx.range(lo, hi)) {
            while (rc.next())
                checker.accept(rc.key());
        }
    }
}
