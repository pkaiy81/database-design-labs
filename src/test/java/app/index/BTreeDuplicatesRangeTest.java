// src/test/java/app/index/BTreeDuplicatesRangeTest.java
package app.index;

import app.storage.FileMgr;
import org.junit.jupiter.api.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BTreeDuplicatesRangeTest extends TestSupport {
    FileMgr fm;
    String dataFile, indexFile;

    @BeforeEach
    void setupEach() {
        fm = newFileMgr();
        dataFile = dataFileName("t4");
        indexFile = indexFileName("idx_t4");
    }

    @Test
    void duplicates_are_all_returned_and_range_order_is_kept() throws Exception {
        try (var idx = newIndex(fm, dataFile, indexFile)) {
            // key=100 に複数RIDを入れる（BlockId固定、slotのみ変える）
            for (int r = 0; r < 50; r++) {
                idx.insert(100, new RID(new app.storage.BlockId(dataFile, 0), r));
            }
            // 近傍キー
            idx.insert(99, ridFor(dataFile, 999));
            idx.insert(101, ridFor(dataFile, 111));

            // 等値検索: 50件すべて返る
            int cnt = 0;
            try (var cur = idx.search(100)) {
                while (cur.next())
                    cnt++;
            }
            assertEquals(50, cnt);

            // レンジ(99..101): 52件返る & 並びは RID.slot の順で [999] → [0..49] → [111]
            java.util.ArrayList<Integer> slots = new java.util.ArrayList<>();
            try (var rc = idx.range(99, 101)) {
                while (rc.next())
                    slots.add(rc.rid().slot()); // RID.slotで検証
            }
            assertEquals(52, slots.size());
            // 先頭は key=99 の slot=999
            assertEquals(999, (int) slots.get(0));
            // 中間 50件は key=100 の slot=0..49
            for (int i = 0; i < 50; i++) {
                assertEquals(i, (int) slots.get(1 + i));
            }
            // 末尾は key=101 の slot=111
            assertEquals(111, (int) slots.get(51));
        }
    }
}
