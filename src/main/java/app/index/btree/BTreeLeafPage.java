package app.index.btree;

import app.index.RID;
import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeLeafPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;
    private final String dataFileName;

    BTreeLeafPage(FileMgr fm, BlockId blk, String dataFileName) {
        this.fm = fm;
        this.page = new BTPage(fm, blk);
        this.dataFileName = dataFileName;
        // ここで format しない（既存ページ破壊防止）
    }

    BlockId block() {
        return page.block();
    }

    int lowerBound(int key) {
        return page.lowerBoundLeaf(key);
    }

    int upperBound(int key) {
        return page.upperBoundLeaf(key);
    }

    void insertAt(int slot, int key, RID rid) {
        page.insertLeafAtRaw(slot, key, rid.block().number(), rid.slot());
        page.flush();
    }

    DirEntry splitAndPromote() throws Exception {
        int splitPos = page.keyCount() / 2;
        int rightFirstKey = page.leafKey(splitPos);
        BlockId rightBlk = page.splitLeaf(splitPos);
        return new DirEntry(rightFirstKey, rightBlk.number());
    }

    int keyAt(int slot) {
        return page.leafKey(slot);
    }

    RID ridAt(int slot) {
        int blockNo = page.leafBlockNo(slot);
        int ridSlot = page.leafRidSlot(slot);
        return new RID(new BlockId(dataFileName, blockNo), ridSlot);
    }

    void removeAt(int slot) {
        page.removeLeafAt(slot);
        page.flush();
    }

    int keyCount() {
        return page.keyCount();
    }

    int capacity() {
        return (fm.blockSize() - BTreeLayouts.HEADER_SIZE) / BTreeLayouts.LEAF_SLOT_SIZE;
    }

    int minKeys() {
        return Math.max(1, capacity() / 2);
    }

    boolean isFull() {
        return keyCount() >= capacity();
    }

    int nextLeafBlockNo() {
        return page.next();
    }

    int prevLeafBlockNo() {
        return page.prev();
    }

    static BTreeLeafPage open(FileMgr fm, String dataFileName, String idxFile, int blockNo) {
        return new BTreeLeafPage(fm, new BlockId(idxFile, blockNo), dataFileName);
    }

    // ---- 借用/マージ系（BTreeIndex.delete から使用） ----
    boolean canBorrowFromLeft(BTreeLeafPage left) {
        return left != null && left.keyCount() > left.minKeys();
    }

    boolean canBorrowFromRight(BTreeLeafPage right) {
        return right != null && right.keyCount() > right.minKeys();
    }

    // 左の末尾1件を取り出して自分の先頭へ挿入
    int borrowFromLeft(BTreeLeafPage left) {
        int posL = left.keyCount() - 1;
        int k = left.keyAt(posL);
        RID r = left.ridAt(posL);
        left.removeAt(posL);
        int ins = lowerBound(k);
        insertAt(ins, k, r);
        return firstKey(); // 親の分割キー（= 右ページ先頭キー）更新用
    }

    // 右の先頭1件を取り出して自分の末尾へ挿入
    int borrowFromRight(BTreeLeafPage right) {
        int k = right.keyAt(0);
        RID r = right.ridAt(0);
        right.removeAt(0);
        int ins = upperBound(k);
        insertAt(ins, k, r);
        return right.firstKey(); // 親の分割キー更新用（右ページの新しい先頭）
    }

    int firstKey() {
        return keyAt(0);
    }

    // 右兄弟をすべて取り込み、leaf リンクを繋ぎ替える。右は空になる前提。
    void mergeWithRight(BTreeLeafPage right) {
        int n = right.keyCount();
        for (int i = 0; i < n; i++) {
            int k = right.keyAt(i);
            RID r = right.ridAt(i);
            insertAt(keyCount(), k, r); // 末尾連結（右は >= 自分の最大）
        }
        // 右リンクの連結更新（BTPage#splitLeaf の実装に準拠した繋ぎ方を踏襲）
        int newNext = right.nextLeafBlockNo();
        this.page.setNext(newNext);
        if (newNext != -1) {
            try (BTPage nxt = new BTPage(fm, new BlockId(block().filename(), newNext))) {
                nxt.setPrev(block().number());
                nxt.flush();
            } catch (Exception ignore) {
            }
        }
        right.close(); // right は以後使わない
    }

    @Override
    public void close() {
        page.close();
    }
}
