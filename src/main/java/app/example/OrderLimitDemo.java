package app.example;

import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.*;
import app.sql.Planner;
import app.storage.FileMgr;

import java.nio.file.Path;

public class OrderLimitDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096;
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);

        // students(id:int, name:string(20))
        String tbl = "students";
        Schema s = new Schema().addInt("id").addString("name", 20);
        mdm.createTable(tbl, s);
        Layout layout = mdm.getLayout(tbl);
        TableFile tf = new TableFile(fm, "students.tbl", layout);

        try (TableScan ts = new TableScan(fm, tf)) {
            ts.enableIndexMaintenance(mdm, tbl);
            ts.insert();
            ts.setInt("id", 2);
            ts.setString("name", "Turing");
            ts.insert();
            ts.setInt("id", 3);
            ts.setString("name", "Dijkstra");
            ts.insert();
            ts.setInt("id", 1);
            ts.setString("name", "Ada");
        }

        Planner planner = new Planner(fm, mdm);

        run(planner, "SELECT name FROM students ORDER BY name ASC");
        run(planner, "SELECT name FROM students ORDER BY name DESC LIMIT 2");
        run(planner, "SELECT id FROM students ORDER BY id ASC LIMIT 2");
    }

    private static void run(Planner planner, String sql) {
        System.out.println("SQL> " + sql);
        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            int n = 0;
            while (s.next()) {
                String name = safe(() -> s.getString("name"));
                String id = safe(() -> Integer.toString(s.getInt("id")));
                System.out.println("  row" + (++n) + ": id=" + id + ", name=" + name);
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
