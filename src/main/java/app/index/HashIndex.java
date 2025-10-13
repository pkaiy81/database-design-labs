package app.index;

import app.record.*;
import app.storage.FileMgr;
import app.storage.BlockId;
import app.storage.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 単純ハッシュインデックス:
 * - 1インデックスにつき bucketCount 個の "バケットファイル" を作成
 * - 各バケットは通常の TableFile(ヒープ)で、エントリは (key:int, blk:int, slot:int)
 * - search(key) は該当バケットを線形スキャンして RID を返す
 */
public final class HashIndex {
    private final FileMgr fm;
    private final String indexName; // 例: "students_id"
    private final String baseTableName; // 例: "students.tbl" (索引対象テーブル)
    private final int bucketCount;
    private final Layout entryLayout;

    public HashIndex(FileMgr fm, String indexName, String baseTableName, int bucketCount) {
        this.fm = Objects.requireNonNull(fm);
        this.indexName = Objects.requireNonNull(indexName);
        this.baseTableName = Objects.requireNonNull(baseTableName);
        if (bucketCount <= 0)
            throw new IllegalArgumentException("bucketCount must be > 0");
        this.bucketCount = bucketCount;

        // インデックスエントリ: key:int, blk:int, slot:int
        Schema s = new Schema()
                .addInt("key")
                .addInt("blk")
                .addInt("slot");
        this.entryLayout = new Layout(s);

        // 各バケットファイルが存在しなければ初期化
        for (int b = 0; b < bucketCount; b++) {
            TableFile tf = bucketFile(b);
            if (tf.size() == 0)
                tf.appendFormatted();
        }
    }

    /** 追加（重複キー許容で複数RIDを格納） */
    public void put(int key, RID rid) {
        int b = bucketOf(key);
        TableFile tf = bucketFile(b);
        try (TableScan scan = new TableScan(fm, tf)) {
            scan.insert();
            scan.setInt("key", key);
            scan.setInt("blk", rid.block().number());
            scan.setInt("slot", rid.slot());
        }
    }

    /** 検索（該当バケットを線形走査） */
    public List<RID> search(int key) {
        int b = bucketOf(key);
        TableFile tf = bucketFile(b);
        List<RID> rids = new ArrayList<>();
        try (TableScan scan = new TableScan(fm, tf)) {
            scan.beforeFirst();
            while (scan.next()) {
                if (scan.getInt("key") == key) {
                    int blk = scan.getInt("blk");
                    int slot = scan.getInt("slot");
                    rids.add(new RID(new BlockId(baseTableName, blk), slot));
                }
            }
        }
        return rids;
    }

    private int bucketOf(int key) {
        int h = Integer.hashCode(key);
        // 非負へ正規化
        if (h == Integer.MIN_VALUE)
            h = 0;
        return Math.floorMod(h, bucketCount);
    }

    private TableFile bucketFile(int b) {
        String fname = bucketFileName(b);
        return new TableFile(fm, fname, entryLayout);
    }

    private String bucketFileName(int b) {
        // 例: idx_students_id_b000.tbl
        return String.format("idx_%s_b%03d.tbl", indexName, b);
    }
}
