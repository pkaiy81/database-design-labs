package app.record;

import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

public final class TableScan implements AutoCloseable {
    private final FileMgr fm;
    private final TableFile tf;

    private int currBlk = -1;
    private RecordPage rp; // 現在のページ
    private int currSlot = -1;

    public TableScan(FileMgr fm, TableFile tf) {
        this.fm = fm;
        this.tf = tf;
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

    private void appendNewBlockAndMove() {
        BlockId b = tf.appendFormatted();
        currBlk = b.number();
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, tf.layout(), fm.blockSize());
        currSlot = -1;
    }

    private void flush() {
        BlockId b = new BlockId(tf.filename(), currBlk);
        // RecordPage が内部で保持している Page を直接取得するAPIを用意していないため、
        // ここは RecordPage 生成時に渡した Page を再利用する方向に作り直すのが自然。
        // シンプルに、RecordPage の持つ Page にアクセスできるように小さな変更を入れます。
        fm.write(b, extractPage(rp));
    }

    private Page extractPage(RecordPage rp) {
        return rp.page();
    }

    @Override
    public void close() {
        /* no resources */ }

    // TableScan に追記（public メソッド）
    // src/main/java/app/record/TableScan.java
    public int currentBlockNumber() {
        return currBlk;
    }

    public int currentSlot() {
        return currSlot;
    }

}
