package app.example;

import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;

import java.nio.file.Path;
import java.util.List;

public class QueryDemo {
    public static void main(String[] args) {
        var dataDir = Path.of("./data");
        int blockSize = 4096; // ← 前章以降と統一
        FileMgr fm = new FileMgr(dataDir, blockSize);
        MetadataManager mdm = new MetadataManager(fm);

        // --- students(id:int, name:string(20)) ---
        String sTbl = "students";
        Schema sSchema = new Schema().addInt("id").addString("name", 20);
        mdm.createTable(sTbl, sSchema);
        Layout sLayout = mdm.getLayout(sTbl);
        TableFile sFile = new TableFile(fm, sTbl + ".tbl", sLayout);

        // データ挿入
        try (TableScan sScan = new TableScan(fm, sFile)) {
            sScan.insert();
            sScan.setInt("id", 1);
            sScan.setString("name", "Ada");
            sScan.insert();
            sScan.setInt("id", 2);
            sScan.setString("name", "Turing");
            sScan.insert();
            sScan.setInt("id", 3);
            sScan.setString("name", "Dijkstra");
        }

        // --- enrollments(student_id:int, course:string(20)) ---
        String eTbl = "enrollments";
        Schema eSchema = new Schema().addInt("student_id").addString("course", 20);
        mdm.createTable(eTbl, eSchema);
        Layout eLayout = mdm.getLayout(eTbl);
        TableFile eFile = new TableFile(fm, eTbl + ".tbl", eLayout);

        try (TableScan eScan = new TableScan(fm, eFile)) {
            eScan.insert();
            eScan.setInt("student_id", 1);
            eScan.setString("course", "DB");
            eScan.insert();
            eScan.setInt("student_id", 2);
            eScan.setString("course", "OS");
            eScan.insert();
            eScan.setInt("student_id", 2);
            eScan.setString("course", "Algo");
        }

        // ==== 1) 選択: students から id=2 ====
        try (Scan sel = new SelectScan(new TableScan(fm, sFile), Predicate.eqInt("id", 2))) {
            sel.beforeFirst();
            System.out.println("[SELECT id=2]");
            while (sel.next()) {
                System.out.println("id=" + sel.getInt("id") + ", name=" + sel.getString("name"));
            }
        }

        // ==== 2) 射影: students から name のみ ====
        try (Scan proj = new ProjectScan(new TableScan(fm, sFile), List.of("name"))) {
            proj.beforeFirst();
            System.out.println("[PROJECT name]");
            while (proj.next()) {
                System.out.println("name=" + proj.getString("name"));
            }
        }

        // ==== 3) 等値結合: students.id = enrollments.student_id ====
        try (Scan join = new SelectScan( // 結合条件
                new ProductScan(
                        new TableScan(fm, sFile),
                        new TableScan(fm, eFile)),
                Predicate.eqField("id", "student_id"))) {
            join.beforeFirst();
            System.out.println("[JOIN students.id = enrollments.student_id]");
            while (join.next()) {
                System.out.println(
                        "id=" + join.getInt("id") +
                                ", name=" + join.getString("name") +
                                ", course=" + join.getString("course"));
            }
        }
    }
}
