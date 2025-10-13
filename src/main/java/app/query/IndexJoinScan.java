package app.query;

import app.index.HashIndex;
import app.index.RID;
import app.record.Layout;
import app.record.RecordPage;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Indexed Nested Loop Join（等値結合・右側INTキーにハッシュ索引）:
 * - 左Scanの各行について、右側インデックスで key を検索し、ヒットRIDsを順に返す
 * - getInt/getStringは左→右の順に解決（左に無ければ右を参照）
 */
public final class IndexJoinScan implements Scan {
    private final Scan left; // 外側
    private final FileMgr fm; // 右側の読込に使う
    private final Layout rightLayout;
    private final String rightTableFile; // 例: "enrollments.tbl"
    private final HashIndex rightIndex; // 右側のハッシュインデックス
    private final String leftKeyField; // 左側：キー列名（例: "id"）
    private final String rightKeyField; // 右側：キー列名（例: "student_id"）

    private List<RID> hits = new ArrayList<>();
    private int pos = -1;
    private RecordPage rightRp;
    private int rightSlot = -1;
    private boolean leftHasRow = false;

    public IndexJoinScan(Scan left, FileMgr fm, Layout rightLayout, String rightTableFile,
            HashIndex rightIndex, String leftKeyField, String rightKeyField) {
        this.left = Objects.requireNonNull(left);
        this.fm = Objects.requireNonNull(fm);
        this.rightLayout = Objects.requireNonNull(rightLayout);
        this.rightTableFile = Objects.requireNonNull(rightTableFile);
        this.rightIndex = Objects.requireNonNull(rightIndex);
        this.leftKeyField = Objects.requireNonNull(leftKeyField);
        this.rightKeyField = Objects.requireNonNull(rightKeyField);
    }

    @Override
    public void beforeFirst() {
        left.beforeFirst();
        leftHasRow = left.next();
        refillHits();
        pos = -1;
        rightRp = null;
        rightSlot = -1;
    }

    @Override
    public boolean next() {
        while (true) {
            if (!leftHasRow)
                return false;
            if (++pos < hits.size()) {
                RID rid = hits.get(pos);
                Page p = new Page(fm.blockSize());
                fm.read(rid.block(), p);
                rightRp = new RecordPage(p, rightLayout, fm.blockSize());
                rightSlot = rid.slot();
                return true;
            }
            // 右が尽きた → 左を進めて右を取り直す
            leftHasRow = left.next();
            refillHits();
            pos = -1;
        }
    }

    private void refillHits() {
        hits = new ArrayList<>();
        if (!leftHasRow)
            return;
        int key = left.getInt(leftKeyField);
        hits = rightIndex.search(key);
    }

    @Override
    public int getInt(String field) {
        try {
            return left.getInt(field);
        } catch (Exception ignore) {
            return rightRp.getInt(rightSlot, field);
        }
    }

    @Override
    public String getString(String field) {
        try {
            return left.getString(field);
        } catch (Exception ignore) {
            return rightRp.getString(rightSlot, field);
        }
    }

    @Override
    public void close() {
        left.close();
    }
}
