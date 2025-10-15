package app.index.btree;

import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeDirPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;

    // 内部ノードレベルのページ
    BTreeDirPage(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.page = new BTPage(fm, blk);
        // ここでの自動formatは厳禁：既存ページを破壊する恐れがある
    }

    BlockId block() {
        return page.block();
    }

    int level() {
        return page.level();
    }

    // key <-> child 選択：SimpleDBの findChildBlock と同等
    // child は slot+1 方向を選択（境界の扱いは「< key は左、>= は右」）
    BlockId findChildBlock(int searchKey) {
        int n = page.keyCount();
        if (n == 0) {
            // ありえないが保険
            return new BlockId(block().filename(), 0);
        }
        // ディレクトリは [minKey, childPtr]
        // => 検索キーに対して最後の key <= searchKey を選ぶ
        int lo = 0, hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (page.dirKey(mid) <= searchKey)
                lo = mid;
            else
                hi = mid - 1;
        }
        int child = page.dirChild(lo);
        return new BlockId(block().filename(), child);
    }

    // 昇格エントリの挿入（満杯なら split してさらに昇格を返す）
    DirEntry insertEntry(DirEntry e) throws Exception {
        int pos = 1 + page.findDirSlotBefore(e.sepKey);
        page.insertDirAt(pos, e.sepKey, e.childBlk);
        // ディレクトリ容量は (blockSize - HEADER)/DIR_SLOT_SIZE 想定
        int capacity = (fm.blockSize() - BTreeLayouts.HEADER_SIZE) / BTreeLayouts.DIR_SLOT_SIZE;
        if (!page.isFullDir(capacity))
            return null;

        // 分割：右ページの先頭キーを昇格キーとして返す
        int splitPos = page.keyCount() / 2;
        int promoteKey = page.dirKey(splitPos);
        var rightBlk = page.splitDir(splitPos);
        return new DirEntry(promoteKey, rightBlk.number());
    }

    private int findSlotLE(BTPage page, int key) {
        int lo = -1, hi = page.keyCount() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (page.dirKey(mid) <= key)
                lo = mid;
            else
                hi = mid - 1;
        }
        return lo; // 最後の <= key
    }

    @Override
    public void close() {
        page.close();
    }
}
