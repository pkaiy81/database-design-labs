// src/main/java/app/memory/BufferMgr.java
package app.memory;

import app.storage.BlockId;
import app.storage.FileMgr;
import java.util.ArrayList;
import java.util.List;

public final class BufferMgr {
    private final List<Buffer> pool;
    private final FileMgr fm;
    private final int blockSize;

    public BufferMgr(FileMgr fm, int blockSize, int numBuffers) {
        this.fm = fm;
        this.blockSize = blockSize;
        this.pool = new ArrayList<>(numBuffers);
        for (int i = 0; i < numBuffers; i++) {
            pool.add(new Buffer(fm, blockSize));
        }
    }

    /** 既存Buffer→未固定Bufferの順で検索。なければ null（＝今回は例外に委ねる想定） */
    public synchronized Buffer pin(BlockId blk) {
        Buffer b = findExisting(blk);
        if (b == null) {
            b = chooseUnpinned();
            if (b == null)
                throw new IllegalStateException("No available buffer to pin");
            b.assignToBlock(blk);
        }
        if (!b.isPinned()) {
            // 既存でも未固定なら pin カウントにより利用開始
        }
        b.pin();
        return b;
    }

    public synchronized void unpin(Buffer b) {
        b.unpin();
    }

    private Buffer findExisting(BlockId blk) {
        for (Buffer b : pool) {
            if (blk.equals(b.block()))
                return b;
        }
        return null;
    }

    /** Naïve: 最初に見つかった未固定を返す（後で FIFO/LRU/Clock に差し替え可能） */
    private Buffer chooseUnpinned() {
        for (Buffer b : pool) {
            if (!b.isPinned())
                return b;
        }
        return null;
    }
}
