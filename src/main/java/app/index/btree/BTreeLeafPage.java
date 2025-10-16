package app.index.btree;

import app.index.RID;
import app.storage.BlockId;
import app.storage.FileMgr;

final class BTreeLeafPage implements AutoCloseable {
    private final FileMgr fm;
    private final BTPage page;
    private final String dataFileName;

    BTreeLeafPage(FileMgr fm, BlockId blk, String dataFileName){
        this.fm = fm;
        this.page = new BTPage(fm, blk);
        this.dataFileName = dataFileName;
        // ここで format しない（既存ページ破壊防止）
    }

    BlockId block(){ return page.block(); }

    int lowerBound(int key){ return page.lowerBoundLeaf(key); }
    int upperBound(int key){ return page.upperBoundLeaf(key); }

    void insertAt(int slot, int key, RID rid){
        page.insertLeafAtRaw(slot, key, rid.block().number(), rid.slot());
	page.flush();
    }

    DirEntry splitAndPromote() throws Exception {
        int splitPos = page.keyCount() / 2;
        int rightFirstKey = page.leafKey(splitPos);
        BlockId rightBlk = page.splitLeaf(splitPos);
        return new DirEntry(rightFirstKey, rightBlk.number());
    }

    int keyAt(int slot){ return page.leafKey(slot); }
    RID ridAt(int slot){
        int blockNo = page.leafBlockNo(slot);
        int ridSlot = page.leafRidSlot(slot);
        return new RID(new BlockId(dataFileName, blockNo), ridSlot);
    }

    void removeAt(int slot){ 
        page.removeLeafAt(slot);
        page.flush();	
    }

    int keyCount(){ return page.keyCount(); }

    int capacity(){
        return (fm.blockSize() - BTreeLayouts.HEADER_SIZE)/BTreeLayouts.LEAF_SLOT_SIZE;
    }
    boolean isFull(){ return keyCount() >= capacity(); }

    int nextLeafBlockNo(){ return page.next(); }
    int prevLeafBlockNo(){ return page.prev(); }

    static BTreeLeafPage open(FileMgr fm, String dataFileName, String idxFile, int blockNo){
        return new BTreeLeafPage(fm, new BlockId(idxFile, blockNo), dataFileName);
    }

    @Override public void close(){ page.close(); }
}

