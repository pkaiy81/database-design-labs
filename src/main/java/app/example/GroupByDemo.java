package app.example;

import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class GroupByDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);

        // scores(student_id:int, score:int)
        String tbl = "scores";
        Schema s = new Schema().addInt("student_id").addString("dummy", 1) // ダミーを1つ（文字列MIN/MAXテスト用にも転用可）
                .addInt("score");
        mdm.createTable(tbl, s);
        Layout layout = mdm.getLayout(tbl);
        TableFile tf = new TableFile(fm, "scores.tbl", layout);
        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, tbl);
            // student_id ごとに複数レコード
            ts.insert();
            ts.setInt("student_id", 1);
            ts.setString("dummy", "a");
            ts.setInt("score", 70);
            ts.insert();
            ts.setInt("student_id", 1);
            ts.setString("dummy", "b");
            ts.setInt("score", 90);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setString("dummy", "c");
            ts.setInt("score", 60);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setString("dummy", "d");
            ts.setInt("score", 80);
            ts.insert();
            ts.setInt("student_id", 2);
            ts.setString("dummy", "e");
            ts.setInt("score", 100);
        }

        Planner planner = new Planner(fm, mdm);

        run(planner, "SELECT student_id, COUNT(*) FROM scores GROUP BY student_id ORDER BY student_id ASC");
        run(planner,
                "SELECT student_id, SUM(score), AVG(score) FROM scores GROUP BY student_id ORDER BY student_id ASC");
        run(planner, "SELECT COUNT(*) FROM scores"); // グローバル集約
        run(planner, "SELECT MIN(score), MAX(score) FROM scores"); // グローバル最小/最大
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int row = 0;
            while (s.next()) {
                // 代表的な列名で出力（存在しない列はスキップ）
                String sid = preferStringThenIntNonEmpty(s, "student_id");
                String cnt = preferIntThenStringNonEmpty(s, "count");
                String sum = preferIntThenStringNonEmpty(s, "sum_score");
                String avg = preferIntThenStringNonEmpty(s, "avg_score");
                String min = preferIntThenStringNonEmpty(s, "min_score");
                String max = preferIntThenStringNonEmpty(s, "max_score");

                System.out.println(
                        "  row" + (++row) +
                                ": student_id=" + sid +
                                ", count=" + cnt +
                                ", sum=" + sum +
                                ", avg=" + avg +
                                ", min=" + min +
                                ", max=" + max);
            }
        }
    }

    private static String preferStringThenIntNonEmpty(app.query.Scan s, String field) {
        try {
            String v = s.getString(field);
            if (v != null && !v.isBlank())
                return v; // ★ 空文字はNGとしてフォールバック
        } catch (Exception ignore) {
        }
        try {
            return Integer.toString(s.getInt(field));
        } catch (Exception ignore) {
        }
        return "-";
    }

    private static String preferIntThenStringNonEmpty(app.query.Scan s, String field) {
        try {
            return Integer.toString(s.getInt(field));
        } catch (Exception ignore) {
        }
        try {
            String v = s.getString(field);
            if (v != null && !v.isBlank())
                return v; // ★ 空文字スキップ
        } catch (Exception ignore) {
        }
        return "-";
    }
}
