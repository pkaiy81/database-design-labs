package app.record;

import app.index.RID;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

// public final class TableScan implements AutoCloseable {
public final class TableScan implements app.query.Scan {
    private final FileMgr fm;
    private final TableFile tf;

    private int currBlk = -1;
    private RecordPage rp; // 現在のページ（レコードI/Oは全部ここ経由）
    private int currSlot = -1;

    public TableScan(FileMgr fm, TableFile tf) {
        this.fm = fm;
        this.tf = tf;
    }

    // 現在行のRIDを返す
    public RID rid() {
        if (currBlk < 0 || currSlot < 0) {
            throw new IllegalStateException("no current record");
        }
        // TableFile(=物理ファイル名)と現在ブロック番号から BlockId を組み立てる
        return new RID(new BlockId(tf.filename(), currBlk), currSlot);
    }

    // 指定RIDへ移動（Index走査→Table参照用）
    public boolean moveTo(RID rid) throws Exception {
        int blkNum = rid.block().number();
        if (rp == null || currBlk != blkNum) {
            if (!moveToBlock(blkNum))
                return false;
        }
        this.currSlot = rid.slot();
        return true;
    }

    public void beforeFirst() {
        currBlk = -1;
        currSlot = -1;
        rp = null;
    }

    /** 次の使用中スロットへ。なければ次ブロックを読み進める。 */
    public boolean next() {
        if (rp == null) {
            if (!moveToBlock(0))
                return false;
        }
        int s = rp.nextUsed(currSlot);
        while (s < 0) {
            if (!moveToBlock(currBlk + 1))
                return false;
            s = rp.nextUsed(-1);
        }
        currSlot = s;
        return true;
    }

    /** 新規レコードを挿入（空スロットがなければ新規ブロックをappend）し、現在位置にする */
    public void insert() {
        if (rp == null && !moveToBlock(0)) {
            appendNewBlockAndMove();
        }
        int s = rp.findFree();
        if (s < 0) {
            appendNewBlockAndMove();
            s = rp.findFree();
            if (s < 0)
                throw new IllegalStateException("no free slot after append");
        }
        rp.setUsed(s, true);
        currSlot = s;
        flush(); // すぐ確定
    }

    public void delete() {
        if (currSlot < 0)
            throw new IllegalStateException("no current record");
        rp.setUsed(currSlot, false);
        flush();
    }

    // フィールドI/O
    public int getInt(String fld) {
        return rp.getInt(currSlot, fld);
    }

    public void setInt(String fld, int v) {
        rp.setInt(currSlot, fld, v);
        flush();
    }

    public String getString(String fld) {
        return rp.getString(currSlot, fld);
    }

    public void setString(String fld, String v) {
        rp.setString(currSlot, fld, v);
        flush();
    }

    /** 指定ブロックへページを読み込み、RecordPage を張り替える */
    private boolean moveToBlock(int blkNum) {
        if (blkNum < 0 || blkNum >= tf.size())
            return false;
        currBlk = blkNum;
        BlockId b = new BlockId(tf.filename(), currBlk);
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, tf.layout(), fm.blockSize());
        currSlot = -1;
        return true;
    }

    /** 新規ブロックを append して、そのブロックへ移動 */
    private void appendNewBlockAndMove() {
        BlockId b = tf.appendFormatted(); // ここで layout に従って空ページを初期化
        currBlk = b.number();
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, tf.layout(), fm.blockSize());
        currSlot = -1;
    }

    /** 現在ページをディスクへ書き戻す */
    private void flush() {
        if (rp == null || currBlk < 0)
            return;
        BlockId b = new BlockId(tf.filename(), currBlk);
        fm.write(b, rp.page()); // ← RecordPage に page() ゲッターを追加（下の最小パッチ参照）
    }

    // hasField
    public boolean hasField(String fldName) {
        return tf.hasField(fldName);
    }

    @Override
    public void close() {
        // 今の設計では明示 flush は各setterで実施済み。必要ならここで後始末を。
    }

    // デバッグ/確認用（任意公開）
    public int currentBlockNumber() {
        return currBlk;
    }

    public int currentSlot() {
        return currSlot;
    }
}
