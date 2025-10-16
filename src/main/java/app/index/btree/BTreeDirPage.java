package app.index.btree;

import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeDirPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;

    BTreeDirPage(FileMgr fm, BlockId blk){
        this.fm = fm;
        this.page = new BTPage(fm, blk);
        // ここで format しない（既存ページ破壊防止）
    }

    BlockId block(){ return page.block(); }

    // 検索キー以下の“最後”のスロットの child に降りる
    BlockId findChildBlock(int searchKey){
        int n = page.keyCount();
        if (n == 0) return new BlockId(block().filename(), 0); // 保険

        int lo = 0, hi = n - 1;
        while (lo < hi){
            int mid = (lo + hi + 1) >>> 1;
            if (page.dirKey(mid) <= searchKey) lo = mid; else hi = mid - 1;
        }
        int child = page.dirChild(lo);
        return new BlockId(block().filename(), child);
    }

    // 昇格エントリの挿入。満杯なら右ページ先頭キーを昇格キーとして返す
    DirEntry insertEntry(DirEntry e) throws Exception {
        int pos = lowerBoundDir(e.sepKey);
        page.insertDirAt(pos, e.sepKey, e.childBlk);
	page.flush();

        int capacity = (fm.blockSize() - BTreeLayouts.HEADER_SIZE) / BTreeLayouts.DIR_SLOT_SIZE;
        if (page.keyCount() <= capacity) return null;

        int splitPos = page.keyCount() / 2;
        int promoteKey = page.dirKey(splitPos);
        BlockId rightBlk = page.splitDir(splitPos);
        return new DirEntry(promoteKey, rightBlk.number());
    }

    private int lowerBoundDir(int key){
        int lo = 0, hi = page.keyCount();
        while (lo < hi){
            int mid = (lo + hi) >>> 1;
            if (page.dirKey(mid) < key) lo = mid + 1; else hi = mid;
        }
        return lo;
    }

    @Override public void close(){ page.close(); }
}

