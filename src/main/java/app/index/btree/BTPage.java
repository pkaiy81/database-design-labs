package app.index.btree;

import app.storage.BlockId;
import app.storage.Page;
import app.storage.FileMgr;

import java.io.Closeable;
import java.io.IOException;

import static app.index.btree.BTreeLayouts.*;

class BTPage implements Closeable {
    private final FileMgr fm;
    private final BlockId blk;
    private final Page p;

    BTPage(FileMgr fm, BlockId blk) {
        this.fm = fm;
        this.blk = blk;
        this.p = new Page(fm.blockSize());
        fm.read(blk, p);
    }

    BlockId block() {
        return blk;
    }

    // --- header
    int level() {
        return p.getInt(OFF_FLAG);
    }

    void setLevel(int v) {
        p.setInt(OFF_FLAG, v);
        dirty();
    }

    int keyCount() {
        return p.getInt(OFF_COUNT);
    }

    void setKeyCount(int n) {
        p.setInt(OFF_COUNT, n);
        dirty();
    }

    int prev() {
        return p.getInt(OFF_PREV);
    }

    void setPrev(int bno) {
        p.setInt(OFF_PREV, bno);
        dirty();
    }

    int next() {
        return p.getInt(OFF_NEXT);
    }

    void setNext(int bno) {
        p.setInt(OFF_NEXT, bno);
        dirty();
    }

    boolean isLeaf() {
        return level() == 0;
    }

    boolean isFullDir(int capacity) {
        return keyCount() >= capacity;
    }

    boolean isFullLeaf(int capacity) {
        return keyCount() >= capacity;
    }

    // --- dir slot access
    private int dirSlotPos(int slot) {
        return HEADER_SIZE + slot * DIR_SLOT_SIZE;
    }

    int dirKey(int slot) {
        return p.getInt(dirSlotPos(slot));
    }

    int dirChild(int slot) {
        return p.getInt(dirSlotPos(slot) + 4);
    }

    void setDirSlot(int slot, int key, int child) {
        int pos = dirSlotPos(slot);
        p.setInt(pos, key);
        p.setInt(pos + 4, child);
        dirty();
    }

    // --- leaf slot access (RID を生 int で持つ)
    private int leafSlotPos(int slot) {
        return HEADER_SIZE + slot * LEAF_SLOT_SIZE;
    }

    int leafKey(int slot) {
        return p.getInt(leafSlotPos(slot));
    }

    int leafBlockNo(int slot) {
        return p.getInt(leafSlotPos(slot) + 4);
    }

    int leafRidSlot(int slot) {
        return p.getInt(leafSlotPos(slot) + 8);
    }

    void setLeafSlotRaw(int slot, int key, int blockNo, int ridSlot) {
        int pos = leafSlotPos(slot);
        p.setInt(pos, key);
        p.setInt(pos + 4, blockNo);
        p.setInt(pos + 8, ridSlot);
        dirty();
    }

    // --- binary search (lower_bound 等)
    int findDirSlotBefore(int key) {
        int lo = -1, hi = keyCount() - 1;
        while (lo < hi) {
            int mid = (lo + hi + 1) >>> 1;
            if (dirKey(mid) < key)
                lo = mid;
            else
                hi = mid - 1;
        }
        return lo;
    }

    int lowerBoundLeaf(int key) {
        int lo = 0, hi = keyCount();
        while (lo < hi) {
            int mid = (lo + hi) >>> 1;
            if (leafKey(mid) < key)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    // --- inserts
    void insertDirAt(int slot, int key, int child) {
        int n = keyCount();
        for (int i = n; i > slot; i--) {
            setDirSlot(i, dirKey(i - 1), dirChild(i - 1));
        }
        setDirSlot(slot, key, child);
        setKeyCount(n + 1);
    }

    void insertLeafAtRaw(int slot, int key, int blockNo, int ridSlot) {
        int n = keyCount();
        for (int i = n; i > slot; i--) {
            int k = leafKey(i - 1);
            int b = leafBlockNo(i - 1);
            int s = leafRidSlot(i - 1);
            setLeafSlotRaw(i, k, b, s);
        }
        setLeafSlotRaw(slot, key, blockNo, ridSlot);
        setKeyCount(n + 1);
    }

    // --- splits
    BlockId splitDir(int splitPos) throws IOException {
        BlockId right = fm.append(blk.filename());
        try (BTPage r = new BTPage(fm, right)) {
            r.setLevel(level());
            r.setKeyCount(0);
            int n = keyCount();
            for (int i = splitPos, j = 0; i < n; i++, j++) {
                r.insertDirAt(j, dirKey(i), dirChild(i));
            }
            setKeyCount(splitPos);
            r.flush();
            return right;
        }
    }

    BlockId splitLeaf(int splitPos) throws IOException {
        BlockId right = fm.append(blk.filename());
        try (BTPage r = new BTPage(fm, right)) {
            r.setLevel(0);
            r.setKeyCount(0);
            int n = keyCount();
            for (int i = splitPos, j = 0; i < n; i++, j++) {
                r.insertLeafAtRaw(j, leafKey(i), leafBlockNo(i), leafRidSlot(i));
            }
            // link maintenance
            r.setNext(next());
            r.setPrev(blk.number());
            if (next() != -1) {
                try (BTPage nxt = new BTPage(fm, new BlockId(blk.filename(), next()))) {
                    nxt.setPrev(right.number());
                    nxt.flush();
                }
            }
            setNext(right.number());
            setKeyCount(splitPos);
            r.flush();
            return right;
        }
    }

    void formatLeaf() {
        setLevel(0);
        setKeyCount(0);
        setPrev(-1);
        setNext(-1);
    }

    void formatDir(int levelVal) {
        setLevel(levelVal);
        setKeyCount(0);
        setPrev(-1);
        setNext(-1);
    }

    void flush() {
        fm.write(blk, p);
    }

    void dirty() {
        /* write-through on flush */ }

    @Override
    public void close() {
        flush();
    }
}
