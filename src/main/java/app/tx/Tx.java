package app.tx;

import app.memory.Buffer;
import app.memory.BufferMgr;
import app.memory.LogManager;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;
import app.tx.lock.LockManager;
import app.tx.lock.LockTable;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public final class Tx implements AutoCloseable {
    private static final AtomicInteger SEQ = new AtomicInteger(1);
    private static final LockTable LOCK_TABLE = new LockTable();

    private final int txId;
    private final FileMgr fm;
    private final BufferMgr bm;
    private final LogManager log;
    private final Path logDir;
    private final LockManager lockMgr;

    public Tx(FileMgr fm, BufferMgr bm, LogManager log, Path logDir) {
        this.txId = SEQ.getAndIncrement();
        this.fm = fm;
        this.bm = bm;
        this.log = log;
        this.logDir = logDir;
        this.lockMgr = new LockManager(LOCK_TABLE);
        // START ログ
        log.append(LogCodec.start(txId));
    }

    public int id() {
        return txId;
    }

    /**
     * 指定されたブロックとオフセットから整数値を読み取ります。
     * 読み取り前に共有ロック（S-Lock）を取得します。
     * 
     * @param blk    読み取り対象のブロック
     * @param offset ブロック内のオフセット
     * @return 読み取った整数値
     */
    public int getInt(BlockId blk, int offset) {
        // 共有ロックを取得
        lockMgr.sLock(blk, txId);

        Buffer buf = bm.pin(blk);
        try {
            return buf.contents().getInt(offset);
        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * 指定されたブロックとオフセットから文字列を読み取ります。
     * 読み取り前に共有ロック（S-Lock）を取得します。
     * 
     * @param blk    読み取り対象のブロック
     * @param offset ブロック内のオフセット
     * @return 読み取った文字列
     */
    public String getString(BlockId blk, int offset) {
        // 共有ロックを取得
        lockMgr.sLock(blk, txId);

        Buffer buf = bm.pin(blk);
        try {
            return buf.contents().getString(offset);
        } finally {
            bm.unpin(buf);
        }
    }

    // setInt: WAL → flush(log) → ページ更新 → dirty → flush(page)
    public void setInt(BlockId blk, int offset, int newVal) {
        // 排他ロックを取得
        lockMgr.xLock(blk, txId);
        Buffer buf = bm.pin(blk);
        try {
            Page p = buf.contents();
            int old = p.getInt(offset);

            // 1) 旧値をログへ
            log.append(LogCodec.setInt(txId, blk.filename(), blk.number(), offset, old));
            log.flush(0); // ログを先に永続化（WAL）

            // 2) ページ更新
            p.setInt(offset, newVal);
            buf.setDirty();

            // 3) データもフラッシュ（サンプルでは即時書き戻しでシンプルに）
            buf.flushIfDirty();

        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * 指定されたブロックとオフセットに文字列を書き込みます。
     * 書き込み前に排他ロック（X-Lock）を取得します。
     * 
     * @param blk    書き込み対象のブロック
     * @param offset ブロック内のオフセット
     * @param newVal 書き込む文字列
     */
    public void setString(BlockId blk, int offset, String newVal) {
        // 排他ロックを取得
        lockMgr.xLock(blk, txId);

        Buffer buf = bm.pin(blk);
        try {
            Page p = buf.contents();
            // 文字列の場合、旧値のログは省略（簡易実装）

            // ページ更新
            p.setString(offset, newVal);
            buf.setDirty();

            // データをフラッシュ
            buf.flushIfDirty();
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

        // すべてのロックを解放（Strict 2PL）
        lockMgr.release(txId);
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
                    buf.flushIfDirty();
                } finally {
                    bm.unpin(buf);
                }
            } else if (parsed.type == LogType.START) {
                break; // ここまででこのTxのUNDO完了
            }
        }
        log.append(LogCodec.rollback(txId));
        log.flush(0);

        // すべてのロックを解放（Strict 2PL）
        lockMgr.release(txId);
    }

    @Override
    public void close() {
        /* no-op */ }
}
