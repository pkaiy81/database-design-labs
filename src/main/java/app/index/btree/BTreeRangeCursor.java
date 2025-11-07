package app.index.btree;

import app.index.RangeCursor;
import app.index.RID;
import app.index.SearchKey;
import app.storage.FileMgr;

final class BTreeRangeCursor implements RangeCursor {
    private final FileMgr fm;
    private final String dataFileName;
    private final String indexFile;
    private final SearchKey low, high;
    private final boolean lowInc, highInc;

    private BTreeLeafPage leaf;
    private int slot;
    private RID currentRid;

    BTreeRangeCursor(
            FileMgr fm, String indexFile, String dataFileName,
            SearchKey low, boolean lowInc, SearchKey high, boolean highInc,
            BTreeLeafPage startLeaf, int startSlot) {
        this.fm = fm;
        this.indexFile = indexFile;
        this.dataFileName = dataFileName;
        this.low = low;
        this.lowInc = lowInc;
        this.high = high;
        this.highInc = highInc;
        this.leaf = startLeaf;
        this.slot = startSlot;
    }

    @Override
    public boolean next() {
        while (true) {
            if (leaf == null)
                return false;
            while (slot < leaf.keyCount()) {
                int k = leaf.keyAt(slot);
                if (!withinLow(k)) {
                    slot++;
                    continue;
                }
                if (!withinHigh(k))
                    return false;
                currentRid = leaf.ridAt(slot);
                slot++;
                return true;
            }
            int nxt = leaf.nextLeafBlockNo();
            leaf.close();
            leaf = (nxt == -1) ? null : BTreeLeafPage.open(fm, dataFileName, indexFile, nxt);
            slot = 0;
        }
    }

    private boolean withinHigh(int key) {
        if (high == null)
            return true;
        int cmp = Integer.compare(key, high.asInt());
        return highInc ? (cmp <= 0) : (cmp < 0);
    }

    private boolean withinLow(int key) {
        if (low == null)
            return true;
        int cmp = Integer.compare(key, low.asInt());
        return lowInc ? (cmp >= 0) : (cmp > 0);
    }

    @Override
    public RID getDataRid() {
        return currentRid;
    }

    @Override
    public void close() {
        if (leaf != null) {
            leaf.close();
            leaf = null;
        }
    }
}
