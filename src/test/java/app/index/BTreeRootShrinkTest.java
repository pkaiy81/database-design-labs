// src/test/java/app/index/BTreeRootShrinkTest.java
package app.index;

import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BTreeRootShrinkTest extends TestSupport {
    FileMgr fm;
    String dataFile, indexFile;

    @BeforeEach
    void setupEach() {
        fm = newFileMgr();
        dataFile = dataFileName("t3");
        indexFile = indexFileName("idx_t3");
    }

    @Test
    void root_shrinks_when_only_child_remains() throws Exception {
        try (var idx = newIndex(fm, dataFile, indexFile)) {
            // 高さが上がる程度に投入
            insertRange(idx, dataFile, 0, 600);

            // ほぼ全削除して葉1枚まで減らす
            for (int i = 0; i < 599; i++) {
                idx.delete(i, ridFor(dataFile, i));
            }

            // 最後の1件が残っている
            try (var c = idx.search(599)) {
                assertTrue(c.next(), "last key should remain");
            }

            // 範囲でも1件のみ
            var keys = collectRangeKeys(idx, Integer.MIN_VALUE, Integer.MAX_VALUE);
            assertEquals(1, keys.size());
            assertEquals(599, keys.get(0).intValue());
        }
    }
}
