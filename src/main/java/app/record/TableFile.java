package app.record;

import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

public final class TableFile {
    private final FileMgr fm;
    private final String filename; // 例: students.tbl
    private final Layout layout;

    public TableFile(FileMgr fm, String filename, Layout layout) {
        this.fm = fm;
        this.filename = filename;
        this.layout = layout;
    }

    public Layout layout() {
        return layout;
    }

    public String filename() {
        return filename;
    }

    public int size() {
        return fm.length(filename);
    }

    /** 新規ブロックを末尾に作成し、空フォーマットする */
    public BlockId appendFormatted() {
        BlockId b = fm.append(filename);
        Page p = new Page(fm.blockSize());
        RecordPage rp = new RecordPage(p, layout, fm.blockSize());
        rp.format();
        fm.write(b, p);
        return b;
    }

    /** 指定ブロックを読み出して RecordPage を返す */
    public RecordPage readPage(BlockId b) {
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        return new RecordPage(p, layout, fm.blockSize());
    }

    /** 書き戻し */
    public void writePage(BlockId b, RecordPage rp) {
        fm.write(b, new Page(rpPageBytes(rp)));
    }

    private byte[] rpPageBytes(RecordPage rp) {
        // RecordPage は内部に Page を持つが直接取り出すAPIは用意していないため、
        // Page.contents() に依存しない版を作るなら RecordPage に "page()" を足しても良い。
        // シンプルにするため、Page への依存を許すコンストラクタを使う別実装にします。
        // ここでは Page を直接受け取る別 write を用意。
        throw new UnsupportedOperationException("use writePage(BlockId, Page) variant");
    }

    /** RecordPageの基となるPageを直接書き戻すためのユーティリティ */
    public void writePage(BlockId b, Page page) {
        fm.write(b, page);
    }
}
