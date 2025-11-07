package app.example;

import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class DistinctHavingDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);

        // names(id:int, name:string(20))
        String t1 = "names";
        Schema s1 = new Schema().addInt("id").addString("name", 20);
        mdm.createTable(t1, s1);
        Layout l1 = mdm.getLayout(t1);
        TableFile f1 = new TableFile(fm, "names.tbl", l1);
        try (TableScan ts = new TableScan(fm, f1)) {
            ts.enableIndexMaintenance(mdm, t1);
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "Ada");
            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "Ada"); // 重複名
            ts.insert();
            ts.setInt("id", 3);
            ts.setString("name", "Turing");
        }

        // scores(student_id:int, score:int)
        String t2 = "scores";
        Schema s2 = new Schema().addInt("student_id").addInt("score");
        mdm.createTable(t2, s2);
        Layout l2 = mdm.getLayout(t2);
        TableFile f2 = new TableFile(fm, "scores.tbl", l2);
        try (TableScan ts = new TableScan(fm, f2)) {
            ts.enableIndexMaintenance(mdm, t2);
            ts.insert();
            ts.setInt("student_id", 1);
            ts.setInt("score", 70);
            ts.insert();
            ts.setInt("student_id", 1);
            ts.setInt("score", 90);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setInt("score", 60);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setInt("score", 80);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setInt("score", 100);
        }

        Planner planner = new Planner(fm, mdm);

        run(planner, "SELECT DISTINCT name FROM names ORDER BY name");
        run(planner,
                "SELECT student_id, COUNT(*) FROM scores GROUP BY student_id HAVING COUNT(*) >= 2 ORDER BY student_id");
        run(planner, "SELECT SUM(score) FROM scores HAVING SUM(score) > 200");
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int i = 0;
            while (s.next()) {
                String name = safe(() -> s.getString("name"));
                String sid = safe(() -> Integer.toString(s.getInt("student_id")));
                String cnt = safe(() -> Integer.toString(s.getInt("count")));
                String sum = safe(() -> Integer.toString(s.getInt("sum_score")));
                System.out.println(
                        "  row" + (++i) + ": name=" + name + ", student_id=" + sid + ", count=" + cnt + ", sum=" + sum);
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
