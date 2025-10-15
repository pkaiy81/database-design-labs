package app.index.btree;

import app.index.RID; // ← ここを app.index に修正
import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeLeafPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;
    private final String dataFileName; // RID 復元に必要（対象テーブルのデータファイル名）

    BTreeLeafPage(FileMgr fm, BlockId blk, String dataFileName) {
        this.fm = fm;
        this.page = new BTPage(fm, blk);
        this.dataFileName = dataFileName;
        if (!page.isLeaf())
            page.formatLeaf();
    }

    BlockId block() {
        return page.block();
    }

    int lowerBound(int key) {
        return page.lowerBoundLeaf(key);
    }

    void insertAt(int slot, int key, RID rid) {
        int blockNo = rid.block().number();
        int ridSlot = rid.slot();
        page.insertLeafAtRaw(slot, key, blockNo, ridSlot);
    }

    DirEntry splitAndPromote() throws Exception {
        int splitPos = page.keyCount() / 2;
        int rightFirstKey = page.leafKey(splitPos);
        var rightBlk = page.splitLeaf(splitPos);
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

    int keyCount() {
        return page.keyCount();
    }

    int capacity() {
        return (fm.blockSize() - BTreeLayouts.HEADER_SIZE) / BTreeLayouts.LEAF_SLOT_SIZE;
    }

    boolean isFull() {
        return keyCount() >= capacity();
    }

    int nextLeafBlockNo() {
        return page.leafNext();
    }

    int prevLeafBlockNo() {
        return page.leafPrev();
    }

    static BTreeLeafPage open(FileMgr fm, String dataFileName, String idxFile, int blockNo) {
        return new BTreeLeafPage(fm, new BlockId(idxFile, blockNo), dataFileName);
    }

    @Override
    public void close() {
        page.close();
    }
}
