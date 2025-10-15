package app.index.btree;

import app.index.Index;
import app.index.RangeCursor;
import app.index.SearchKey;
import app.index.RID; // ← ここを app.index に修正
import app.storage.BlockId;
import app.storage.FileMgr;

public final class BTreeIndex implements Index {
    private final FileMgr fm;
    private final String indexFile;
    private final String dataFileName; // RID 復元のために必要

    private BlockId root;
    private BTreeLeafPage leaf;
    private int slot = -1;
    private int currKey;

    public BTreeIndex(FileMgr fm, String indexFile, String dataFileName) throws Exception {
        this.fm = fm;
        this.indexFile = indexFile;
        this.dataFileName = dataFileName;

        if (fm.length(indexFile) == 0) {
            root = fm.append(indexFile);
            try (BTPage p = new BTPage(fm, root)) {
                p.formatLeaf();
                p.flush();
            }
        } else {
            root = new BlockId(indexFile, 0);
        }
    }

    @Override
    public void open() {
        /* no-op */ }

    @Override
    public void beforeFirst(SearchKey key) {
        closeLeafIfAny();
        BlockId child = root;
        while (true) {
            try (BTPage p = new BTPage(fm, child)) {
                if (p.isLeaf())
                    break;
            }
            try (BTreeDirPage dir = new BTreeDirPage(fm, child)) {
                child = dir.findChildBlock(key.asInt());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        leaf = new BTreeLeafPage(fm, child, dataFileName); // ← dataFileName を渡す
        slot = leaf.lowerBound(key.asInt());
        currKey = key.asInt();
    }

    @Override
    public boolean next() {
        if (leaf == null)
            return false;
        while (slot < leaf.keyCount()) {
            int k = leaf.keyAt(slot);
            if (k != currKey)
                return false;
            return true;
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        RID r = leaf.ridAt(slot);
        slot++;
        return r;
    }

    private void growNewRootFromLeaf(DirEntry up, BlockId leftLeaf) throws Exception {
        // 新ルートを作り、左右の葉を指す
        BlockId newRoot = fm.append(indexFile);
        try (BTPage p = new BTPage(fm, newRoot)) {
            p.formatDir(1);
            p.setKeyCount(0);
            try (BTreeDirPage dir = new BTreeDirPage(fm, newRoot)) {
                dir.insertEntry(new DirEntry(Integer.MIN_VALUE, leftLeaf.number()));
                dir.insertEntry(up);
            }
        }
        // ルート更新
        this.root = newRoot;
    }

    private void growNewRootFromDir(DirEntry up, BlockId oldRoot) throws Exception {
        BlockId newRoot = fm.append(indexFile);
        try (BTPage p = new BTPage(fm, newRoot)) {
            p.formatDir(2);
            p.setKeyCount(0);
            try (BTreeDirPage dir = new BTreeDirPage(fm, newRoot)) {
                dir.insertEntry(new DirEntry(Integer.MIN_VALUE, oldRoot.number()));
                dir.insertEntry(up);
            }
        }
        this.root = newRoot;
    }

    @Override
    public void insert(SearchKey key, RID rid) {
        try {
            // 分割情報を受け取りつつ再帰挿入
            DirEntry up = insertRec(root, key.asInt(), rid);

            if (up == null)
                return; // ルート成長不要

            if (up != null) {
                try (BTPage rootPage = new BTPage(fm, root)) {
                    if (rootPage.isLeaf()) {
                        // ルートが葉だった → 葉2枚をぶら下げる新ルートを新規作成
                        growNewRootFromLeaf(up, root);
                    } else {
                        DirEntry up2;
                        try (BTreeDirPage dir = new BTreeDirPage(fm, root)) {
                            up2 = dir.insertEntry(up); // ルートに入れてみる
                        }
                        if (up2 != null) {
                            // ルートも溢れた → 旧ルートと up2.child をぶら下げる新ルートを新規作成
                            growNewRootFromDir(up2, root);
                        }
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DirEntry insertRec(BlockId blk, int key, RID rid) throws Exception {
        try (BTPage raw = new BTPage(fm, blk)) {
            if (raw.isLeaf()) {
                try (BTreeLeafPage lf = new BTreeLeafPage(fm, blk, dataFileName)) {
                    int pos = lf.lowerBound(key);
                    lf.insertAt(pos, key, rid);
                    if (!lf.isFull())
                        return null;
                    return lf.splitAndPromote();
                }
            } else {
                try (BTreeDirPage dir = new BTreeDirPage(fm, blk)) {
                    BlockId child = dir.findChildBlock(key);
                    DirEntry childUp = insertRec(child, key, rid);
                    if (childUp == null)
                        return null;
                    return dir.insertEntry(childUp);
                }
            }
        }
    }

    @Override
    public void delete(SearchKey key, RID rid) {
        // 1) key の葉へ降下
        BlockId child = root;
        while (true) {
            try (BTPage p = new BTPage(fm, child)) {
                if (p.isLeaf())
                    break;
            }
            try (BTreeDirPage dir = new BTreeDirPage(fm, child)) {
                child = dir.findChildBlock(key.asInt());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        try (BTreeLeafPage lf = new BTreeLeafPage(fm, child, dataFileName)) {
            int pos = lf.lowerBound(key.asInt());
            // 同一キー範囲を線形に見て、RID 一致を削除
            while (pos < lf.keyCount() && lf.keyAt(pos) == key.asInt()) {
                RID r = lf.ridAt(pos);
                if (r.equals(rid)) {
                    lf.removeAt(pos);
                    return; // 1件だけ削除（重複が複数ある場合は呼び出し側で複数回呼ぶ）
                }
                pos++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public RangeCursor range(SearchKey low, boolean lowInc, SearchKey high, boolean highInc) {
        // 1) 起点となる key（low があれば low、なければ最小相当）を使って葉へ降下
        int startKey = (low != null) ? low.asInt() : Integer.MIN_VALUE;

        BlockId child = root;
        while (true) {
            try (BTPage p = new BTPage(fm, child)) {
                if (p.isLeaf())
                    break;
            }
            try (BTreeDirPage dir = new BTreeDirPage(fm, child)) {
                child = dir.findChildBlock(startKey);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        BTreeLeafPage startLeaf = new BTreeLeafPage(fm, child, dataFileName);
        int startSlot = startLeaf.lowerBound(startKey);

        return new BTreeRangeCursor(
                fm, indexFile, dataFileName, low, lowInc, high, highInc, startLeaf, startSlot);
    }

    private void closeLeafIfAny() {
        if (leaf != null) {
            leaf.close();
            leaf = null;
        }
    }

    @Override
    public void close() {
        closeLeafIfAny();
    }
}
