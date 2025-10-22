package app.query;

import app.index.RangeCursor;
import app.index.RID;
import app.index.SearchKey;
import app.index.btree.BTreeIndex;
import app.record.TableFile;
import app.record.TableScan;
import app.storage.FileMgr;

import java.util.List;

/**
 * Scan that iterates records in the natural order of a B+tree index, optionally bounded
 * by low/high keys and applying residual predicates plus a LIMIT style cap.
 */
public final class IndexOrderScan implements Scan {
    private final FileMgr fm;
    private final TableFile tableFile;
    private final String indexName;
    private final SearchKey lowKey;
    private final boolean lowInclusive;
    private final SearchKey highKey;
    private final boolean highInclusive;
    private final List<Predicate> residualPredicates;
    private final int limit;

    private TableScan tableScan;
    private BTreeIndex index;
    private RangeCursor cursor;
    private int emitted;

    public IndexOrderScan(FileMgr fm,
                          TableFile tableFile,
                          String indexName,
                          SearchKey lowKey,
                          boolean lowInclusive,
                          SearchKey highKey,
                          boolean highInclusive,
                          List<Predicate> residualPredicates,
                          int limit) {
        this.fm = fm;
        this.tableFile = tableFile;
        this.indexName = indexName;
        this.lowKey = lowKey;
        this.lowInclusive = lowInclusive;
        this.highKey = highKey;
        this.highInclusive = highInclusive;
        this.residualPredicates = residualPredicates == null ? List.of() : List.copyOf(residualPredicates);
        this.limit = Math.max(0, limit);
    }

    @Override
    public void beforeFirst() {
        closeCursor();
        try {
            if (tableScan == null)
                tableScan = new TableScan(fm, tableFile);
            tableScan.beforeFirst();
            index = new BTreeIndex(fm, indexName, tableFile.filename());
            index.open();
            cursor = index.range(lowKey, lowInclusive, highKey, highInclusive);
            emitted = 0;
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize IndexOrderScan", e);
        }
    }

    @Override
    public boolean next() {
        if (cursor == null)
            throw new IllegalStateException("beforeFirst() must be called before next()");
        if (emitted >= limit)
            return false;
        try {
            while (cursor.next()) {
                RID rid = cursor.getDataRid();
                if (!tableScan.moveTo(rid))
                    continue;
                if (matchesResidualPredicates()) {
                    emitted++;
                    return true;
                }
            }
            return false;
        } catch (Exception e) {
            throw new RuntimeException("Index-order scan failed", e);
        }
    }

    private boolean matchesResidualPredicates() {
        for (Predicate predicate : residualPredicates) {
            if (!predicate.evaluate(tableScan))
                return false;
        }
        return true;
    }

    @Override
    public int getInt(String field) {
        return tableScan.getInt(field);
    }

    @Override
    public String getString(String field) {
        return tableScan.getString(field);
    }

    @Override
    public void close() {
        closeCursor();
        if (tableScan != null) {
            tableScan.close();
            tableScan = null;
        }
    }

    private void closeCursor() {
        if (cursor != null) {
            try {
                cursor.close();
            } catch (Exception ignore) {
            }
            cursor = null;
        }
        if (index != null) {
            try {
                index.close();
            } catch (Exception ignore) {
            }
            index = null;
        }
    }
}
