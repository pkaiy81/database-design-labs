// src/main/java/app/memory/Buffer.java
package app.memory;

import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

public final class Buffer {
    private final FileMgr fm;
    private Page contents;
    private BlockId blk;
    private int pins = 0;
    private boolean dirty = false;

    public Buffer(FileMgr fm, int blockSize) {
        this.fm = fm;
        this.contents = new Page(blockSize);
    }

    public synchronized void assignToBlock(BlockId b) {
        flushIfDirty();
        this.blk = b;
        fm.read(b, contents);
        this.pins = 0;
        this.dirty = false;
    }

    public synchronized void flushIfDirty() {
        if (dirty && blk != null) {
            fm.write(blk, contents);
            dirty = false;
        }
    }

    public synchronized void pin() {
        pins++;
    }

    public synchronized void unpin() {
        if (pins > 0)
            pins--;
    }

    public synchronized boolean isPinned() {
        return pins > 0;
    }

    public synchronized void setDirty() {
        this.dirty = true;
    }

    public synchronized Page contents() {
        return contents;
    }

    public synchronized BlockId block() {
        return blk;
    }
}
