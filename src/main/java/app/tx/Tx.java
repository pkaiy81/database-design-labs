package app.tx;

import app.memory.Buffer;
import app.memory.BufferMgr;
import app.memory.LogManager;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class Tx implements AutoCloseable {
    private static final AtomicInteger SEQ = new AtomicInteger(1);

    private final int txId;
    private final FileMgr fm;
    private final BufferMgr bm;
    private final LogManager log;
    private final Path logDir;

    public Tx(FileMgr fm, BufferMgr bm, LogManager log, Path logDir) {
        this.txId = SEQ.getAndIncrement();
        this.fm = fm;
        this.bm = bm;
        this.log = log;
        this.logDir = logDir;
        // START ログ
        log.append(LogCodec.start(txId));
    }

    public int id() {
        return txId;
    }

    /** WAL順序で整数を書き換える：ログ→ログflush→ページ更新 */
    public void setInt(BlockId blk, int offset, int newVal) {
        Buffer buf = bm.pin(blk);
        try {
            Page p = buf.contents();
            int old = p.getInt(offset);

            // 1) ログ（旧値）を先に永続化
            log.append(LogCodec.setInt(txId, blk.filename(), blk.number(), offset, old));
            log.flush(0); // ここでは簡略化：常にforce

            // 2) ページを更新してdirtyに
            p.setInt(offset, newVal);
            buf.setDirty();
        } finally {
            bm.unpin(buf);
        }
    }

    /** commit: ここでは dirty を明示 flush → COMMIT ログ */
    public void commit() {
        // 実運用のWALでは「ログ先flush→データflush」の順。
        // このサンプルでは setInt 時点でログflush済みなので、ここではページ側の書き戻しを促す形に。
        // シンプルのため Buffer が持つ flushIfDirty を使うなら、置換時/終了時に呼ばれる想定。
        log.append(LogCodec.commit(txId));
        log.flush(0);
    }

    /** rollback: 自Txのログを後ろ向きに辿り、SET_INT を元に戻す */
    public void rollback() {
        List<byte[]> all = new LogReader(logDir).readAll();
        for (int i = all.size() - 1; i >= 0; i--) {
            var parsed = LogCodec.parse(all.get(i));
            if (parsed.txId != txId)
                continue;
            if (parsed.type == LogType.SET_INT) {
                BlockId b = new BlockId(parsed.filename, parsed.blk);
                Buffer buf = bm.pin(b);
                try {
                    buf.contents().setInt(parsed.offset, parsed.oldVal);
                    buf.setDirty();
                } finally {
                    bm.unpin(buf);
                }
            } else if (parsed.type == LogType.START) {
                break; // ここまででこのTxのUNDO完了
            }
        }
        log.append(LogCodec.rollback(txId));
        log.flush(0);
    }

    @Override
    public void close() {
        /* no-op */ }
}
