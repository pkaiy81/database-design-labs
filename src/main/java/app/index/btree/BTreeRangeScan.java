package app.index.btree;

import app.index.RangeCursor;
import app.index.RID;
import app.index.SearchKey;
import app.query.Scan;
import app.record.TableScan;

/**
 * B-Tree レンジ走査の Scan 実装。
 * RangeCursor に beforeFirst() は無い前提。
 * - RangeCursor: next(), getDataRid(), close() のみ使用
 * - RID: app.index.RID
 * - TableScan に値取得を委譲
 */
public final class BTreeRangeScan implements Scan {
    private final TableScan ts;
    private final RangeCursor cursor;

    private RID currentRid;
    private boolean started = false;

    public BTreeRangeScan(
            TableScan ts,
            BTreeIndex index,
            SearchKey low, boolean lowInclusive,
            SearchKey high, boolean highInclusive) {
        this.ts = ts;
        this.cursor = index.range(low, lowInclusive, high, highInclusive);
    }

    @Override
    public void beforeFirst() {
        // RangeCursor に beforeFirst は無いので内部状態のみ初期化
        this.started = false;
        this.currentRid = null;
    }

    @Override
    public boolean next() {
        if (!started)
            started = true; // 最初の next() が先頭へ進む
        if (!cursor.next()) {
            currentRid = null;
            return false;
        }
        currentRid = cursor.getDataRid();
        try {
            if (!ts.moveTo(currentRid)) {
                throw new IllegalStateException("Failed to move TableScan to RID: " + currentRid);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error moving TableScan to RID: " + currentRid, e);
        }
        return true;
    }

    public boolean hasField(String fldName) {
        return ts.hasField(fldName);
    }

    @Override
    public int getInt(String fldName) {
        return ts.getInt(fldName);
    }

    @Override
    public String getString(String fldName) {
        return ts.getString(fldName);
    }

    // 他に getLong/getDouble/getVal などが Scan に存在する場合は同様に委譲してください

    @Override
    public void close() {
        try {
            cursor.close();
        } finally {
            ts.close();
        }
    }
}
