package app.index.btree;

import app.index.Index;
import app.index.RangeCursor;
import app.index.SearchKey;
import app.index.RID;
import app.storage.BlockId;
import app.storage.FileMgr;

public final class BTreeIndex implements Index {
    private final FileMgr fm;
    private final String indexFile;
    private final String dataFileName;

    private BlockId root;
    private BTreeLeafPage leaf;
    private int slot = -1;
    private int currKey;
    private RID bufferedRid;

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
    }

    @Override
    public void beforeFirst(SearchKey key) {
        closeLeafIfAny();
        BlockId child = descendToLeaf(key.asInt());
        leaf = new BTreeLeafPage(fm, child, dataFileName);
        slot = leaf.lowerBound(key.asInt());
        currKey = key.asInt();
        bufferedRid = null;
    }

    @Override
    public boolean next() {
        if (leaf == null)
            return false;
        // 等値キー（currKey）の範囲内で1つ前進しつつバッファに積む
        while (slot < leaf.keyCount()) {
            int k = leaf.keyAt(slot);
            if (k != currKey)
                return false; // 同一キー領域を抜け
            bufferedRid = leaf.ridAt(slot);
            slot++; // ここで1つ前進しておく
            return true;
        }
        return false;
    }

    @Override
    public RID getDataRid() {
        return bufferedRid;
    }

    @Override
    public void insert(SearchKey key, RID rid) {
        try {
            DirEntry up = insertRec(root, key.asInt(), rid);
            if (up == null)
                return;

            try (BTPage rp = new BTPage(fm, root)) {
                if (rp.isLeaf()) {
                    // ルートが葉 → 新ルートを append して差し替え
                    growNewRootFromLeaf(up, root);
                } else {
                    DirEntry up2;
                    try (BTreeDirPage dir = new BTreeDirPage(fm, root)) {
                        up2 = dir.insertEntry(up);
                    }
                    if (up2 != null) {
                        growNewRootFromDir(up2, root);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private DirEntry insertRec(BlockId blk, int key, RID rid) throws Exception {
        // 1) まずは isLeaf だけを確認してすぐ閉じる（ダブルオープンを避ける）
        boolean isLeaf;
        try (BTPage meta = new BTPage(fm, blk)) {
            isLeaf = meta.isLeaf();
        }

        if (isLeaf) {
            // 2) 葉だけを開く（このスコープで唯一の開き先）
            try (BTreeLeafPage lf = new BTreeLeafPage(fm, blk, dataFileName)) {
                int pos = lf.upperBound(key);
                lf.insertAt(pos, key, rid); // insertAt 内で flush 済み
                if (!lf.isFull())
                    return null;
                return lf.splitAndPromote(); // splitLeaf 内でも flush 済み
            }
        } else {
            // 3) 内部だけを開く（このスコープで唯一の開き先）
            try (BTreeDirPage dir = new BTreeDirPage(fm, blk)) {
                BlockId child = dir.findChildBlock(key);
                DirEntry childUp = insertRec(child, key, rid);
                if (childUp == null)
                    return null;
                return dir.insertEntry(childUp); // insertEntry でも flush 済み
            }
        }
    }

    // ルートが葉だった場合の成長：新規ルート(dir, level=1)を作り、[-∞→旧葉], [up] を挿入
    private void growNewRootFromLeaf(DirEntry up, BlockId leftLeaf) throws Exception {
        BlockId newRoot = fm.append(indexFile);
        try (BTPage p = new BTPage(fm, newRoot)) {
            // 単一のBTPageで完結させる（他のインスタンスで同ブロックを開かない）
            p.formatDir(1);
            // [-∞ -> 旧leaf], [up.sepKey -> up.childBlk] の2エントリを直接設定
            p.setDirSlot(0, Integer.MIN_VALUE, leftLeaf.number());
            p.setDirSlot(1, up.sepKey, up.childBlk);
            p.setKeyCount(2);
            p.flush();
        }
        this.root = newRoot;
    }

    // ルートが内部ノードだった場合の成長：新規ルート(dir, level=2)を作り、[-∞→旧root], [up] を挿入
    private void growNewRootFromDir(DirEntry up, BlockId oldRoot) throws Exception {
        BlockId newRoot = fm.append(indexFile);
        try (BTPage p = new BTPage(fm, newRoot)) {
            // 単一のBTPageで完結させる
            p.formatDir(2);
            p.setDirSlot(0, Integer.MIN_VALUE, oldRoot.number());
            p.setDirSlot(1, up.sepKey, up.childBlk);
            p.setKeyCount(2);
            p.flush();
        }
        this.root = newRoot;
    }

    // delete 後にカーソルを無効化（= 直後の next() は false を返すようにする） ---
    @Override
    public void delete(SearchKey key, RID rid) {
        BlockId child = descendToLeaf(key.asInt());
        try (BTreeLeafPage lf = new BTreeLeafPage(fm, child, dataFileName)) {
            int pos = lf.lowerBound(key.asInt());
            while (pos < lf.keyCount() && lf.keyAt(pos) == key.asInt()) {
                RID r = lf.ridAt(pos);
                if (r.equals(rid)) {
                    lf.removeAt(pos);
                    // ここで内部カーソルを無効化（テスト: delete直後の next() は false になる）
                    closeLeafIfAny();
                    slot = -1;
                    return;
                }
                pos++;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private BlockId descendToLeaf(int key) {
        BlockId child = root;
        while (true) {
            try (BTPage p = new BTPage(fm, child)) {
                if (p.isLeaf())
                    return child;
            }
            try (BTreeDirPage dir = new BTreeDirPage(fm, child)) {
                child = dir.findChildBlock(key);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public RangeCursor range(SearchKey low, boolean lowInc, SearchKey high, boolean highInc) {
        int startKey = (low != null) ? low.asInt() : Integer.MIN_VALUE;
        BlockId child = descendToLeaf(startKey);
        BTreeLeafPage startLeaf = new BTreeLeafPage(fm, child, dataFileName);
        int startSlot = startLeaf.lowerBound(startKey);
        return new BTreeRangeCursor(fm, indexFile, dataFileName, low, lowInc, high, highInc, startLeaf, startSlot);
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

    public static String physicalName(String indexName) {
        return indexName;
    }

    public static boolean drop(FileMgr fm, String indexName) {
        boolean removed = fm.deleteFileIfExists(physicalName(indexName));
        boolean removedWithExt = fm.deleteFileIfExists(indexName + ".idx");
        return removed || removedWithExt;
    }
}
