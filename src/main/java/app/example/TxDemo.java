package app.example;

import app.memory.*;
import app.storage.*;
import app.tx.Tx;

import java.nio.file.Path;

public class TxDemo {
    public static void main(String[] args) {
        // 第3章（FileMgr）＆ 第4章（BufferMgr, LogManager）を利用
        var dataDir = Path.of("./data");
        var logDir = Path.of("./data"); // 同じ場所に simpledb.log
        int blockSize = 400;

        FileMgr fm = new FileMgr(dataDir, blockSize);
        BufferMgr bm = new BufferMgr(fm, blockSize, 4);
        try (LogManager log = new LogManager(logDir)) {

            // ターゲットブロックを準備
            String fname = "acct.tbl";
            BlockId blk = (fm.length(fname) == 0) ? fm.append(fname) : new BlockId(fname, 0);

            // 初期値0を書き出し（Txを使わず直接）
            var init = bm.pin(blk);
            init.contents().setInt(0, 0);
            init.setDirty();
            init.flushIfDirty();
            bm.unpin(init);

            // 1) Tx1: +100 して commit
            try (Tx tx1 = new Tx(fm, bm, log, logDir)) {
                tx1.setInt(blk, 0, 100);
                tx1.commit();
                System.out.println("After tx1 commit: " + readInt(fm, blk));
            }

            // 2) Tx2: +200 するが rollback（→ 100 に戻るはず）
            try (Tx tx2 = new Tx(fm, bm, log, logDir)) {
                tx2.setInt(blk, 0, 200);
                System.out.println("During tx2 (before rollback): " + readInt(fm, blk));
                tx2.rollback();
                System.out.println("After tx2 rollback: " + readInt(fm, blk));
            }
        }
    }

    private static int readInt(FileMgr fm, BlockId blk) {
        Page p = new Page(fm.blockSize());
        fm.read(blk, p);
        return p.getInt(0);
    }
}
