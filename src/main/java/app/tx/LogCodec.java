package app.tx;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** ログのシリアライズ/デシリアライズ（固定ヘッダ＋可変ペイロード） */
public final class LogCodec {
    // レコード共通ヘッダ: [type:int][txId:int]
    public static byte[] start(int txId) {
        var bb = ByteBuffer.allocate(8);
        bb.putInt(LogType.START.code).putInt(txId);
        return bb.array();
    }

    public static byte[] commit(int txId) {
        var bb = ByteBuffer.allocate(8);
        bb.putInt(LogType.COMMIT.code).putInt(txId);
        return bb.array();
    }

    public static byte[] rollback(int txId) {
        var bb = ByteBuffer.allocate(8);
        bb.putInt(LogType.ROLLBACK.code).putInt(txId);
        return bb.array();
    }

    /**
     * SET_INT:
     * [type:int][txId:int][fnameLen:int][fname:utf8][blk:int][offset:int][oldVal:int]
     */
    public static byte[] setInt(int txId, String filename, int blk, int offset, int oldVal) {
        var nameBytes = filename.getBytes(StandardCharsets.UTF_8);
        var bb = ByteBuffer.allocate(4 + 4 + 4 + nameBytes.length + 4 + 4 + 4);
        bb.putInt(LogType.SET_INT.code).putInt(txId);
        bb.putInt(nameBytes.length).put(nameBytes);
        bb.putInt(blk).putInt(offset).putInt(oldVal);
        return bb.array();
    }

    /** 単純なパーサ（必要範囲のみ提供） */
    public static Parsed parse(byte[] rec) {
        var bb = ByteBuffer.wrap(rec);
        var type = LogType.from(bb.getInt());
        var txId = bb.getInt();
        if (type == LogType.SET_INT) {
            int nlen = bb.getInt();
            byte[] nb = new byte[nlen];
            bb.get(nb);
            String fname = new String(nb, StandardCharsets.UTF_8);
            int blk = bb.getInt();
            int offset = bb.getInt();
            int oldVal = bb.getInt();
            return Parsed.setInt(txId, fname, blk, offset, oldVal);
        } else {
            return Parsed.simple(type, txId);
        }
    }

    /** デシリアライズ結果のコンテナ */
    public static final class Parsed {
        public final LogType type;
        public final int txId;
        public final String filename;
        public final Integer blk;
        public final Integer offset;
        public final Integer oldVal;

        private Parsed(LogType t, int txId, String f, Integer b, Integer off, Integer ov) {
            this.type = t;
            this.txId = txId;
            this.filename = f;
            this.blk = b;
            this.offset = off;
            this.oldVal = ov;
        }

        public static Parsed simple(LogType t, int txId) {
            return new Parsed(t, txId, null, null, null, null);
        }

        public static Parsed setInt(int txId, String f, int b, int off, int ov) {
            return new Parsed(LogType.SET_INT, txId, f, b, off, ov);
        }
    }
}
