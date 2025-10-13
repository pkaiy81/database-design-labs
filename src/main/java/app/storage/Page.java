package app.storage;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** 固定長ページのバイト配列に対する便利メソッド群 */
public final class Page {
    private final ByteBuffer bb;

    /** 新規に blockSize バイトの空ページを確保 */
    public Page(int blockSize) {
        if (blockSize <= 0)
            throw new IllegalArgumentException("blockSize must be > 0");
        this.bb = ByteBuffer.allocate(blockSize);
    }

    /** 既存の配列をラップ（FileMgr.read/write と連携） */
    public Page(byte[] data) {
        if (data == null || data.length == 0)
            throw new IllegalArgumentException("data is empty");
        this.bb = ByteBuffer.wrap(data);
    }

    /** 基礎配列（FileMgr が read/write で利用） */
    public byte[] contents() {
        return bb.array();
    }

    /** int の読み書き（バイトオフセットは0始まり） */
    public int getInt(int offset) {
        return bb.getInt(offset);
    }

    public void setInt(int offset, int val) {
        bb.putInt(offset, val);
    }

    /**
     * 文字列は [長さ(int)][UTF-8本体] の2部構成で格納
     * 呼び出し側で十分な空き領域を確保してください
     */
    public String getString(int offset) {
        int len = getInt(offset);
        if (len < 0)
            throw new IllegalStateException("negative string length");
        byte[] dst = new byte[len];
        System.arraycopy(bb.array(), offset + Integer.BYTES, dst, 0, len);
        return new String(dst, StandardCharsets.UTF_8);
    }

    public void setString(int offset, String s) {
        byte[] src = s.getBytes(StandardCharsets.UTF_8);
        setInt(offset, src.length);
        System.arraycopy(src, 0, bb.array(), offset + Integer.BYTES, src.length);
    }
}
