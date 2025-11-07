// src/test/java/app/index/BTreeBorrowUpdatesParentKeyTest.java
package app.index;

import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class BTreeBorrowUpdatesParentKeyTest extends TestSupport {
    FileMgr fm;
    String dataFile, indexFile;

    @BeforeEach
    void setupEach() {
        fm = newFileMgr();
        dataFile = dataFileName("t2");
        indexFile = indexFileName("idx_t2");
    }

    @Test
    void borrow_updates_parent_separator() throws Exception {
        try (var idx = newIndex(fm, dataFile, indexFile)) {
            // 葉が複数枚になる程度に挿入
            insertRange(idx, dataFile, 0, 400);

            // 左側を多めに削除して "左→右借用" を誘発（閾値はブロックサイズ依存）
            for (int i = 0; i < 180; i++) {
                idx.delete(i, ridFor(dataFile, i));
            }

            // 親の分割キー更新が効いていれば、この近傍は正しく見つかる
            for (int i = 180; i < 190; i++) {
                try (var c = idx.search(i)) {
                    assertTrue(c.next(), "key " + i + " should be found after borrow+update");
                }
            }
        }
    }
}
