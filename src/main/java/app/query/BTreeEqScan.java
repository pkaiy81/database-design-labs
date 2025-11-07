// src/main/java/app/query/BTreeEqScan.java
package app.query;

import app.metadata.MetadataManager;
import app.record.Layout;
import app.record.TableFile;
import app.record.TableScan;
import app.index.RID;
import app.storage.FileMgr;

// B+木
import app.index.SearchKey;
import app.index.btree.BTreeIndex;
import app.index.RangeCursor;

/**
 * WHERE 等値 (=) を B+木の range(k, true, k, true) で実現する Scan。
 */
public final class BTreeEqScan implements Scan {
    private final FileMgr fm;
    private final MetadataManager mdm;
    private final String table;
    private final String indexName;
    private final int key;

    private BTreeIndex idx;
    private RangeCursor cur; // range(k,k) のカーソル
    private TableFile tf;
    private TableScan ts;

    public BTreeEqScan(FileMgr fm, MetadataManager mdm, String table, String indexName, int key) {
        this.fm = fm;
        this.mdm = mdm;
        this.table = table;
        this.indexName = indexName;
        this.key = key;
    }

    @Override
    public void beforeFirst() {
        close();
        try {
            // 1) 索引を開く（あなたのコンストラクタ順に合わせています）
            this.idx = new BTreeIndex(fm, indexName, table + ".tbl");
            this.idx.open();

            // 2) 等値は range(k, true, k, true)
            SearchKey sk = SearchKey.ofInt(key);
            this.cur = idx.range(sk, /* lowInc= */true, sk, /* highInc= */true);

            // 3) テーブル側（RID で moveTo）
            Layout layout = mdm.getLayout(table);
            this.tf = new TableFile(fm, table + ".tbl", layout);
            this.ts = new TableScan(fm, tf);
            this.ts.beforeFirst();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean next() {
        try {
            while (cur.next()) {
                RID rid = cur.getDataRid();
                if (ts.moveTo(rid))
                    return true;
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // SimpleIJ が使う getter を委譲（必要に応じて追加）
    @Override
    public int getInt(String fldName) {
        return ts.getInt(fldName);
    }

    @Override
    public String getString(String fldName) {
        return ts.getString(fldName);
    }

    @Override
    public void close() {
        try {
            if (cur != null)
                cur.close();
        } catch (Exception ignore) {
        }
        try {
            if (idx != null)
                idx.close();
        } catch (Exception ignore) {
        }
        try {
            if (ts != null)
                ts.close();
        } catch (Exception ignore) {
        }
        cur = null;
        idx = null;
        ts = null;
    }
}
