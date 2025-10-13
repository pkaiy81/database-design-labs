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

/** HashIndex で key を引いて得た RID 群を、順に読み出す Scan */
public final class IndexSelectScan implements Scan {
    private final FileMgr fm;
    private final Layout layout;
    private final String tableFile;
    private final HashIndex index;
    private final int key;

    private List<RID> hits = new ArrayList<>();
    private int pos = -1;
    private RecordPage rp;
    private int slot = -1;

    public IndexSelectScan(FileMgr fm, Layout layout, String tableFile, HashIndex index, int key) {
        this.fm = fm;
        this.layout = layout;
        this.tableFile = tableFile;
        this.index = index;
        this.key = key;
    }

    @Override
    public void beforeFirst() {
        hits = index.search(key);
        pos = -1;
        rp = null;
        slot = -1;
    }

    @Override
    public boolean next() {
        if (++pos >= hits.size())
            return false;
        RID rid = hits.get(pos);
        BlockId b = rid.block();
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, layout, fm.blockSize());
        slot = rid.slot();
        return true;
    }

    @Override
    public int getInt(String field) {
        return rp.getInt(slot, field);
    }

    @Override
    public String getString(String field) {
        return rp.getString(slot, field);
    }

    @Override
    public void close() {
        /* no resources */ }
}
