package app.index.btree;

import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeDirPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;

    BTreeDirPage(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.page = new BTPage(fm, blk); // 既存ページを開くだけ（format禁止）
    }

    // ---- 基本アクセサ（BTreeIndex/テストから使えるよう露出） ----
    BlockId block() {
        return page.block();
    }

    int level() {
        return page.level();
    }

    int keyCount() {
        return page.keyCount();
    }

    int keyAt(int i) {
        return page.dirKey(i);
    }

    int childAt(int i) {
        return page.dirChild(i);
    }

    // ---- 子探索：最大の (key_i <= searchKey) の child を選ぶ ----
    BlockId findChildBlock(int searchKey) {
        int n = page.keyCount();
        if (n == 0) {
            // ここでブロック0にフォールバックしていたのがバグの根源。
            // ディレクトリページが空なのは構造不変条件違反なので、例外にします。
            throw new IllegalStateException("Directory page has no entries: " + block());
        }
        // floor(<=) を二分探索
        int lo = 0, hi = n - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (page.dirKey(mid) <= searchKey)
                lo = mid;
            else
                hi = mid - 1;
        }
        int childNo = page.dirChild(lo);
        return new BlockId(block().filename(), childNo);
    }

    // ---- 昇格エントリ挿入：満杯なら split し、右ページ先頭キーを昇格して返す ----
    DirEntry insertEntry(DirEntry e) throws Exception {
        // lower_bound: 初めて dirKey(i) >= e.sepKey となる位置
        int pos = lowerBoundDir(e.sepKey);
        page.insertDirAt(pos, e.sepKey, e.childBlk);
        // 容量チェック（BTPage に isFull が無い前提なので自前計算）
        int capacity = (fm.blockSize() - BTreeLayouts.HEADER_SIZE) / BTreeLayouts.DIR_SLOT_SIZE;
        if (page.keyCount() <= capacity) {
            page.flush(); // 即時反映
            return null;
        }
        // 分割：右ページの先頭キーを昇格キーにする
        int splitPos = page.keyCount() / 2;
        int promoteKey = page.dirKey(splitPos);
        BlockId right = page.splitDir(splitPos); // 右側は内部で flush 済み想定
        page.flush(); // 左側も念のため
        return new DirEntry(promoteKey, right.number());
    }

    // ---- 親側ユーティリティ（借用/マージで使用） ----
    int findChildIndexForKey(int searchKey) {
        // 親の分割キーは「右ページの先頭キー」。lower_bound 的に検索。
        int lo = 0, hi = page.keyCount();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (page.dirKey(mid) < searchKey)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo; // 最初に key >= searchKey となる位置（= 右ページのエントリ）
    }

    void replaceSepKeyAt(int idx, int newSepKey) {
        int child = page.dirChild(idx);
        page.setDirSlot(idx, newSepKey, child);
        page.flush();
    }

    void removeEntryAt(int idx) {
        page.removeDirAt(idx);
        page.flush();
    }

    private int lowerBoundDir(int key) {
        int lo = 0, hi = page.keyCount();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (page.dirKey(mid) < key)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    @Override
    public void close() {
        page.close();
    }
}
