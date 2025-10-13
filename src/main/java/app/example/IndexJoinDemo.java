package app.example;

import app.index.IndexRegistry;
import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class IndexJoinDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);
        IndexRegistry reg = new IndexRegistry(fm);

        // students(id:int, name:string(20))
        Schema sSchema = new Schema().addInt("id").addString("name", 20);
        mdm.createTable("students", sSchema);
        Layout sLayout = mdm.getLayout("students");
        TableFile sFile = new TableFile(fm, "students.tbl", sLayout);
        try (TableScan ts = new TableScan(fm, sFile)) {
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

        // enrollments(student_id:int, course:string(20))
        Schema eSchema = new Schema().addInt("student_id").addString("course", 20);
        mdm.createTable("enrollments", eSchema);
        Layout eLayout = mdm.getLayout("enrollments");
        TableFile eFile = new TableFile(fm, "enrollments.tbl", eLayout);
        try (TableScan ts = new TableScan(fm, eFile)) {
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

        // --- 1) 索引なしプラン ---
        Planner noIdx = new Planner(fm, mdm);
        run(noIdx, "SELECT name, course FROM students JOIN enrollments ON id = student_id");

        // --- 2) enrollments.student_id に索引を作って INLJ を使う ---
        var idx = reg.createHashIndex("enrollments", "student_id", eLayout, 17);
        reg.buildHashIndex("enrollments", "student_id", eFile, eLayout);

        Planner withIdx = new Planner(fm, mdm, reg);
        run(withIdx, "SELECT name, course FROM students JOIN enrollments ON id = student_id");
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int n = 0;
            while (s.next()) {
                System.out.println("  name=" + s.getString("name") + ", course=" + s.getString("course"));
                n++;
            }
            System.out.println("  rows=" + n);
        }
    }
}
