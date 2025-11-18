package app.tx;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    /**
     * SET_STRING (Phase 2):
     * [type:int][txId:int][fnameLen:int][fname:utf8][blk:int][offset:int][oldValLen:int][oldVal:utf8]
     */
    public static byte[] setString(int txId, String filename, int blk, int offset, String oldVal) {
        var nameBytes = filename.getBytes(StandardCharsets.UTF_8);
        var valBytes = oldVal.getBytes(StandardCharsets.UTF_8);
        var bb = ByteBuffer.allocate(4 + 4 + 4 + nameBytes.length + 4 + 4 + 4 + valBytes.length);
        bb.putInt(LogType.SET_STRING.code).putInt(txId);
        bb.putInt(nameBytes.length).put(nameBytes);
        bb.putInt(blk).putInt(offset);
        bb.putInt(valBytes.length).put(valBytes);
        return bb.array();
    }

    /**
     * CHECKPOINT (Phase 2):
     * [type:int][0:int][txCount:int][tx1:int][tx2:int]...
     * txIdは0（チェックポイントはトランザクションに属さない）
     */
    public static byte[] checkpoint(List<Integer> activeTxIds) {
        var bb = ByteBuffer.allocate(4 + 4 + 4 + activeTxIds.size() * 4);
        bb.putInt(LogType.CHECKPOINT.code).putInt(0); // txId = 0
        bb.putInt(activeTxIds.size());
        for (int txId : activeTxIds) {
            bb.putInt(txId);
        }
        return bb.array();
    }

    /** 単純なパーサ（必要範囲のみ提供） */
    public static Parsed parse(byte[] rec) {
        var bb = ByteBuffer.wrap(rec);
        var type = LogType.from(bb.getInt());
        var txId = bb.getInt();

        switch (type) {
            case SET_INT: {
                int nlen = bb.getInt();
                byte[] nb = new byte[nlen];
                bb.get(nb);
                String fname = new String(nb, StandardCharsets.UTF_8);
                int blk = bb.getInt();
                int offset = bb.getInt();
                int oldVal = bb.getInt();
                return Parsed.setInt(txId, fname, blk, offset, oldVal);
            }
            case SET_STRING: {
                int nlen = bb.getInt();
                byte[] nb = new byte[nlen];
                bb.get(nb);
                String fname = new String(nb, StandardCharsets.UTF_8);
                int blk = bb.getInt();
                int offset = bb.getInt();
                int vlen = bb.getInt();
                byte[] vb = new byte[vlen];
                bb.get(vb);
                String oldVal = new String(vb, StandardCharsets.UTF_8);
                return Parsed.setString(txId, fname, blk, offset, oldVal);
            }
            case CHECKPOINT: {
                int txCount = bb.getInt();
                var activeTxIds = new java.util.ArrayList<Integer>(txCount);
                for (int i = 0; i < txCount; i++) {
                    activeTxIds.add(bb.getInt());
                }
                return Parsed.checkpoint(activeTxIds);
            }
            default:
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
        public final Integer oldValInt; // SET_INTの旧値
        public final String oldValString; // SET_STRINGの旧値
        public final List<Integer> activeTxIds; // CHECKPOINTのアクティブTx

        private Parsed(LogType t, int txId, String f, Integer b, Integer off,
                Integer ovi, String ovs, List<Integer> active) {
            this.type = t;
            this.txId = txId;
            this.filename = f;
            this.blk = b;
            this.offset = off;
            this.oldValInt = ovi;
            this.oldValString = ovs;
            this.activeTxIds = active;
        }

        public static Parsed simple(LogType t, int txId) {
            return new Parsed(t, txId, null, null, null, null, null, null);
        }

        public static Parsed setInt(int txId, String f, int b, int off, int ov) {
            return new Parsed(LogType.SET_INT, txId, f, b, off, ov, null, null);
        }

        public static Parsed setString(int txId, String f, int b, int off, String ov) {
            return new Parsed(LogType.SET_STRING, txId, f, b, off, null, ov, null);
        }

        public static Parsed checkpoint(List<Integer> activeTxIds) {
            return new Parsed(LogType.CHECKPOINT, 0, null, null, null, null, null, activeTxIds);
        }
    }
}
