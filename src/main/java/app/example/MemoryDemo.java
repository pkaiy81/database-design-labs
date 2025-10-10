// src/main/java/app/example/MemoryDemo.java
package app.example;

import app.memory.*;
import app.storage.*;
import java.nio.file.Path;

public class MemoryDemo {
    public static void main(String[] args) {
        // 第3章の実装を利用
        FileMgr fm = new FileMgr(Path.of("./data"), 400);
        BufferMgr bm = new BufferMgr(fm, fm.blockSize(), 3); // 3 枚のバッファ

        // 1) ファイルへ空ブロックを append
        String fname = "students.tbl";
        BlockId blk = fm.append(fname);

        // 2) バッファに pin → ページを書き換え → dirty → unpin
        Buffer buf = bm.pin(blk);
        Page p = buf.contents();
        int offId = 0;
        int offName = offId + Integer.BYTES;
        p.setInt(offId, 1);
        p.setString(offName, "Ada Lovelace");
        buf.setDirty();
        bm.unpin(buf); // ここでは unpin しても自動 flush しない

        // 3) 読み出し確認（別バッファで pin → read）
        Buffer buf2 = bm.pin(blk); // 既存バッファが拾われる or 未固定から割当
        Page q = buf2.contents(); // 前回 dirty を flush していないので、Buffer が同一なら内容は保持
        System.out.println("id=" + q.getInt(offId) + ", name=" + q.getString(offName));
        bm.unpin(buf2);

        // 4) 明示的に flush（終了前）
        // 今回はデモの単純化のため Buffer 側で flush を呼ぶ
        buf.flushIfDirty(); // 実運用では BufferMgr や Tx 終了時にまとめて flush します

        // 5) 別プロセス相当の読み直し（本当にディスクに出たか確認）
        Page r = new Page(fm.blockSize());
        fm.read(blk, r);
        System.out.println("disk: id=" + r.getInt(offId) + ", name=" + r.getString(offName));
        System.out.println("blocks in file = " + fm.length(fname));
    }
}
