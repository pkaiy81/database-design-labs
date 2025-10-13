package app.metadata;

import app.record.*;
import app.storage.FileMgr;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * システムカタログ管理:
 * - tblcat(tblname:string(64), slotsize:int)
 * - fldcat(tblname:string(64), fldname:string(64), type:int, length:int,
 * offset:int)
 *
 * type: 0=INT, 1=STRING
 * length: STRINGの最大バイト(≒ maxChars*4) / INTは0
 */
public final class MetadataManager {

    private final FileMgr fm;

    // カタログのレイアウト（固定）
    private final Layout tblcatLayout;
    private final Layout fldcatLayout;

    // カタログのテーブルファイル
    private final TableFile tblcat;
    private final TableFile fldcat;

    public MetadataManager(FileMgr fm) {
        this.fm = fm;

        // tblcat レイアウト
        Schema t = new Schema()
                .addString("tblname", 64)
                .addInt("slotsize");
        this.tblcatLayout = new Layout(t);

        // fldcat レイアウト
        Schema f = new Schema()
                .addString("tblname", 64)
                .addString("fldname", 64)
                .addInt("type")
                .addInt("length")
                .addInt("offset");
        this.fldcatLayout = new Layout(f);

        this.tblcat = new TableFile(fm, "tblcat.tbl", tblcatLayout);
        this.fldcat = new TableFile(fm, "fldcat.tbl", fldcatLayout);

        // 初回起動時の空ページ確保（ファイルが0ブロックなら1ブロック作る）
        if (tblcat.size() == 0)
            tblcat.appendFormatted();
        if (fldcat.size() == 0)
            fldcat.appendFormatted();
    }

    /** ユーザー定義テーブルの作成（カタログにレコード追加） */
    public void createTable(String tblname, Schema schema) {
        // Layout を一度作って recordSize を求める
        Layout layout = new Layout(schema);
        int slotSize = layout.recordSize();

        // tblcat へ1件
        try (TableScan scan = new TableScan(fm, tblcat)) {
            scan.insert();
            scan.setString("tblname", tblname);
            scan.setInt("slotsize", slotSize);
        }

        // fldcat へ各フィールドを展開
        for (Map.Entry<String, Schema.FieldDef> e : schema.fields().entrySet()) {
            String fld = e.getKey();
            FieldType ft = e.getValue().type;
            int typeCode = (ft == FieldType.INT ? 0 : 1);
            int len = (ft == FieldType.INT) ? 0 : layout.maxStringBytes(fld);
            int off = layout.offset(fld);

            try (TableScan scan = new TableScan(fm, fldcat)) {
                scan.insert();
                scan.setString("tblname", tblname);
                scan.setString("fldname", fld);
                scan.setInt("type", typeCode);
                scan.setInt("length", len);
                scan.setInt("offset", off);
            }
        }
    }

    /** カタログから Layout を復元 */
    public Layout getLayout(String tblname) {
        // tblcat のエントリ確認（slotsize は情報用途。実際の計算は fldcat から復元）
        boolean found = false;
        try (TableScan scan = new TableScan(fm, tblcat)) {
            scan.beforeFirst();
            while (scan.next()) {
                if (tblname.equals(scan.getString("tblname"))) {
                    found = true;
                    break;
                }
            }
        }
        if (!found)
            throw new IllegalArgumentException("table not found: " + tblname);

        // fldcat から列を復元
        Map<String, FieldType> types = new LinkedHashMap<>();
        Map<String, Integer> strMaxBytes = new LinkedHashMap<>();
        Map<String, Integer> offsets = new LinkedHashMap<>();

        try (TableScan scan = new TableScan(fm, fldcat)) {
            scan.beforeFirst();
            while (scan.next()) {
                if (!tblname.equals(scan.getString("tblname")))
                    continue;
                String fld = scan.getString("fldname");
                int type = scan.getInt("type");
                int length = scan.getInt("length");
                int offset = scan.getInt("offset");

                FieldType ft = (type == 0) ? FieldType.INT : FieldType.STRING;
                types.put(fld, ft);
                if (ft == FieldType.STRING)
                    strMaxBytes.put(fld, length);
                offsets.put(fld, offset);
            }
        }

        // Schema を再構築（追加順の安定性のため LinkedHashMap 前提）
        Schema schema = new Schema();
        for (var e : types.entrySet()) {
            String fld = e.getKey();
            if (e.getValue() == FieldType.INT) {
                schema.addInt(fld);
            } else {
                // 文字数→バイト換算の逆算は困難なので、ここでは端的に「バイト数/4」を最大文字数とする
                int maxBytes = strMaxBytes.getOrDefault(fld, 4);
                int maxChars = Math.max(1, maxBytes / 4);
                schema.addString(fld, maxChars);
            }
        }
        // Layout は新規計算（offset は一致する想定）
        return new Layout(schema);
    }
}
