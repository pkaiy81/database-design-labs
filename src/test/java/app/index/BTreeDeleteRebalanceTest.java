// src/test/java/app/index/BTreeDeleteRebalanceTest.java
package app.index;

import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class BTreeDeleteRebalanceTest extends TestSupport {
    FileMgr fm;
    String dataFile, indexFile;

    @BeforeEach
    void setupEach() {
        fm = newFileMgr();
        dataFile = dataFileName("t1");
        indexFile = indexFileName("idx_t1");
    }

    @Test
    void delete_many_then_search_is_consistent_and_range_keeps_order() throws Exception {
        try (var idx = newIndex(fm, dataFile, indexFile)) {
            // 1) 0..999 を挿入
            insertRange(idx, dataFile, 0, 1000);

            // 2) 偶数キーを削除（下限違反 → 借用/マージが発生する領域）
            for (int i = 0; i < 1000; i += 2) {
                idx.delete(i, ridFor(dataFile, i));
            }

            // 3) 等値：偶数消え・奇数残り
            assertOddRemainEvenGone(idx, 0, 1000);

            // 4) レンジ：1..999 の奇数が昇順
            int[] expected = java.util.stream.IntStream.rangeClosed(1, 999).filter(x -> (x & 1) == 1).toArray();
            var got = collectRangeKeys(idx, 1, 999);
            assertArrayEquals(expected, got.stream().mapToInt(Integer::intValue).toArray());
        }
    }
}
