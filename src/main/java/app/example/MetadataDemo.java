package app.example;

import app.metadata.MetadataManager;
import app.record.*;
import app.storage.FileMgr;

import java.nio.file.Path;

public class MetadataDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);

        // メタデータ管理の起動（カタログを初期化）
        MetadataManager mdm = new MetadataManager(fm);

        // 1) CREATE TABLE students(id:int, name:string(20))
        Schema schema = new Schema()
                .addInt("id")
                .addString("name", 20);
        mdm.createTable("students", schema);

        // 2) getLayout でレイアウト復元
        Layout layout = mdm.getLayout("students");

        // 3) TableFile & TableScan でデータ挿入・走査
        TableFile tf = new TableFile(fm, "students.tbl", layout);
        try (TableScan scan = new TableScan(fm, tf)) {
            scan.enableIndexMaintenance(mdm, "students");
            scan.insert();
            scan.setInt("id", 1);
            scan.setString("name", "Ada");

            scan.insert();
            scan.setInt("id", 2);
            scan.setString("name", "Turing");

            scan.beforeFirst();
            while (scan.next()) {
                System.out.println("id=" + scan.getInt("id") + ", name=" + scan.getString("name"));
            }
        }
    }
}
