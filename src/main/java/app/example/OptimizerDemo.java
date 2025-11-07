package app.example;

import app.index.IndexRegistry;
import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class OptimizerDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);
        IndexRegistry registry = new IndexRegistry(fm);

        // students(id:int, name:string(20)) を用意
        String tbl = "students";
        Schema schema = new Schema().addInt("id").addString("name", 20);
        mdm.createTable(tbl, schema);
        Layout layout = mdm.getLayout(tbl);
        TableFile tf = new TableFile(fm, tbl + ".tbl", layout);

        // データ投入（少し多め）
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, tbl);
            for (int i = 1; i <= 20; i++) {
                ts.insert();
                ts.setInt("id", i % 7); // 0..6 の重複キー
                ts.setString("name", "user" + i);
            }
        }

        // --- まずは索引なしで実行 ---
        Planner plannerNoIdx = new Planner(fm, mdm);
        run(plannerNoIdx, "SELECT name FROM students WHERE id = 2"); // フルスキャン＋選択

        // --- 索引を作成・構築して実行（id にハッシュ索引） ---
        var idx = registry.createHashIndex("students", "id", layout, 17);
        registry.buildHashIndex("students", "id", tf, layout);

        Planner plannerWithIdx = new Planner(fm, mdm, registry);
        run(plannerWithIdx, "SELECT name FROM students WHERE id = 2"); // ← IndexSelectScan に置換される
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int n = 0;
            while (s.next()) {
                System.out.println("  name=" + s.getString("name"));
                n++;
            }
            System.out.println("  rows=" + n);
        }
    }
}
