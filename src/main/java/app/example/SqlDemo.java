package app.example;

import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class SqlDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);

        // スキーマ作成 & データ投入（存在しない前提の簡易サンプル）
        Schema sSchema = new Schema().addInt("id").addString("name", 20);
        mdm.createTable("students", sSchema);
        Schema eSchema = new Schema().addInt("student_id").addString("course", 20);
        mdm.createTable("enrollments", eSchema);

        // データ挿入
        TableFile sFile = new TableFile(fm, "students.tbl", new Layout(sSchema));
        try (TableScan ts = new TableScan(fm, sFile)) {
            ts.enableIndexMaintenance(mdm, "students");
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "Ada");
            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "Turing");
            ts.insert();
            ts.setInt("id", 3);
            ts.setString("name", "Dijkstra");
        }
        TableFile eFile = new TableFile(fm, "enrollments.tbl", new Layout(eSchema));
        try (TableScan ts = new TableScan(fm, eFile)) {
            ts.enableIndexMaintenance(mdm, "enrollments");
            ts.insert();
            ts.setInt("student_id", 1);
            ts.setString("course", "DB");
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setString("course", "OS");
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setString("course", "Algo");
        }

        Planner planner = new Planner(fm, mdm);

        // 1) 単表：WHERE + 射影
        run(planner, "SELECT name FROM students WHERE id = 2");

        // 2) 結合：JOIN ... ON ... + 射影
        run(planner, "SELECT name, course FROM students JOIN enrollments ON id = student_id WHERE name = 'Turing'");
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int row = 0;
            while (s.next()) {
                // デモでは name / course / id のいずれかを出してみる（存在しない列は無視）
                String name = safe(() -> s.getString("name"));
                String course = safe(() -> s.getString("course"));
                String id = safe(() -> Integer.toString(s.getInt("id")));
                System.out.println("  row" + (++row) + ": id=" + id + ", name=" + name + ", course=" + course);
            }
        }
    }

    private static String safe(java.util.concurrent.Callable<String> c) {
        try {
            return c.call();
        } catch (Exception e) {
            return "-";
        }
    }
}
