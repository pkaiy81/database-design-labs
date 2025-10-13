package app.example;

import app.record.*;
import app.storage.FileMgr;

import java.nio.file.Path;

public class RecordDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;

        FileMgr fm = new FileMgr(dataDir, blockSize);

        // スキーマ: id:int, name:string(20)
        Schema schema = new Schema()
                .addInt("id")
                .addString("name", 20);
        Layout layout = new Layout(schema);

        String fname = "students.tbl";
        TableFile tf = new TableFile(fm, fname, layout);

        try (TableScan scan = new TableScan(fm, tf)) {
            // 2件挿入
            scan.insert();
            scan.setInt("id", 1);
            scan.setString("name", "Ada");

            scan.insert();
            scan.setInt("id", 2);
            scan.setString("name", "Turing");

            // 全件走査
            scan.beforeFirst();
            while (scan.next()) {
                System.out.println("id=" + scan.getInt("id") + ", name=" + scan.getString("name"));
            }
        }
    }
}
