package app.example;

import app.index.HashIndex;
import app.index.RID;
import app.metadata.MetadataManager;
import app.record.*;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

import java.nio.file.Path;
import java.util.List;

public class IndexDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096; // ★ 前章の修正に合わせて 4KB 推奨
        FileMgr fm = new FileMgr(dataDir, blockSize);

        // --- メタデータ: students(id:int, name:string(20)) を作成 ---
        MetadataManager mdm = new MetadataManager(fm);
        String tblName = "students";
        String tblFile = tblName + ".tbl";

        Schema schema = new Schema().addInt("id").addString("name", 20);
        mdm.createTable(tblName, schema);
        Layout layout = mdm.getLayout(tblName);

        // --- テーブルへ挿入しながら RID を取り、インデックスに put ---
        TableFile tf = new TableFile(fm, tblFile, layout);
        HashIndex idx = new HashIndex(fm, "students_id", tblFile, 17); // 17バケット

        try (TableScan scan = new TableScan(fm, tf)) {
            // 1件目
            scan.insert();
            scan.setInt("id", 1);
            scan.setString("name", "Ada");
            RID rid1 = new RID(new BlockId(tblFile, scan.currentBlockNumber()), scan.currentSlot());
            idx.put(1, rid1);

            // 2件目
            scan.insert();
            scan.setInt("id", 2);
            scan.setString("name", "Turing");
            RID rid2 = new RID(new BlockId(tblFile, scan.currentBlockNumber()), scan.currentSlot());
            idx.put(2, rid2);

            // 3件目（重複キー例：id=2 をもう1件）
            scan.insert();
            scan.setInt("id", 2);
            scan.setString("name", "Knuth");
            RID rid3 = new RID(new BlockId(tblFile, scan.currentBlockNumber()), scan.currentSlot());
            idx.put(2, rid3);
        }

        // --- インデックス検索（id=2）→ RID 経由で元レコードを読む ---
        List<RID> hits = idx.search(2);
        System.out.println("id=2 -> RID count: " + hits.size());
        for (RID rid : hits) {
            // RID から直接ページを読み、レコードを取り出す
            Page p = new Page(fm.blockSize());
            fm.read(rid.block(), p);
            RecordPage rp = new RecordPage(p, layout, fm.blockSize());
            int id = rp.getInt(rid.slot(), "id");
            String name = rp.getString(rid.slot(), "name");
            System.out.println("RID=" + rid + " -> id=" + id + ", name=" + name);
        }
    }
}
