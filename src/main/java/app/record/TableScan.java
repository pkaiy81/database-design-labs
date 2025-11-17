package app.record;

import app.index.RID;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;
import app.index.SearchKey;
import app.index.btree.BTreeIndex;
import app.metadata.MetadataManager;
import app.tx.Tx;
import app.tx.lock.IsolationLevel;

// public final class TableScan implements AutoCloseable {
public final class TableScan implements app.query.Scan {
    private final FileMgr fm;
    private final TableFile tf;
    private final Tx tx; // トランザクションインスタンス（ロック管理用）

    private int currBlk = -1;
    private RecordPage rp; // 現在のページ（レコードI/Oは全部ここ経由）
    private int currSlot = -1;

    /**
     * トランザクション統合版コンストラクタ
     * 
     * @param tx トランザクションインスタンス（ロック取得に使用）
     * @param tf テーブルファイル
     */
    public TableScan(Tx tx, TableFile tf) {
        this.tx = tx;
        this.fm = tx.getFileMgr();
        this.tf = tf;
    }

    /**
     * 後方互換性のためのコンストラクタ（トランザクションなし）
     * 注意: ロック機能を使用しない場合のみ使用
     * 
     * @deprecated Txを使用するコンストラクタを推奨
     */
    @Deprecated
    public TableScan(FileMgr fm, TableFile tf) {
        this.tx = null;
        this.fm = fm;
        this.tf = tf;
    }

    // 現在行のRIDを返す
    public RID rid() {
        if (currBlk < 0 || currSlot < 0) {
            throw new IllegalStateException("no current record");
        }
        // TableFile(=物理ファイル名)と現在ブロック番号から BlockId を組み立てる
        return new RID(new BlockId(tf.filename(), currBlk), currSlot);
    }

    // 指定RIDへ移動（Index走査→Table参照用）
    public boolean moveTo(RID rid) throws Exception {
        int blkNum = rid.block().number();
        if (rp == null || currBlk != blkNum) {
            if (!moveToBlock(blkNum))
                return false;
        }
        this.currSlot = rid.slot();
        return true;
    }

    public void beforeFirst() {
        currBlk = -1;
        currSlot = -1;
        rp = null;
    }

    /**
     * 次の使用中スロットへ。なければ次ブロックを読み進める。
     * トランザクション有効時：読み取り前に共有ロック（S-Lock）を取得（分離レベルに応じて）
     */
    public boolean next() {
        if (rp == null) {
            if (!moveToBlock(0))
                return false;
        }
        int s = rp.nextUsed(currSlot);
        while (s < 0) {
            if (!moveToBlock(currBlk + 1))
                return false;
            s = rp.nextUsed(-1);
        }
        currSlot = s;

        // トランザクション有効時：現在ブロックにロックを取得
        if (tx != null && tx.getIsolationLevel().usesReadLocks()) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            // Txのロック機構を利用（内部でLockManagerを使用）
            // 分離レベルに応じて、READ_COMMITTEDなら後で解放、REPEATABLE_READなら保持
            acquireReadLock(blk);
        }

        return true;
    }

    /**
     * 読み取りロックを取得するヘルパーメソッド
     */
    private void acquireReadLock(BlockId blk) {
        if (tx == null)
            return;

        IsolationLevel level = tx.getIsolationLevel();
        if (!level.usesReadLocks())
            return;

        // Txを経由してロック取得（内部でsLock→buffer pin→unpinを実行）
        // ここでは直接LockManagerを使用
        tx.getInt(blk, 0); // ダミー読み取りでロックを取得

        // READ_COMMITTED の場合は既に unlock されている（Tx.getInt 内で処理）
        // REPEATABLE_READ / SERIALIZABLE の場合は commit まで保持
    }

    /**
     * 新規レコードを挿入（空スロットがなければ新規ブロックをappend）し、現在位置にする
     * トランザクション有効時：挿入前に排他ロック（X-Lock）を取得
     */
    public void insert() {
        if (rp == null && !moveToBlock(0)) {
            appendNewBlockAndMove();
        }
        int s = rp.findFree();
        if (s < 0) {
            appendNewBlockAndMove();
            s = rp.findFree();
            if (s < 0)
                throw new IllegalStateException("no free slot after append");
        }

        // トランザクション有効時：排他ロックを取得
        if (tx != null) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireWriteLock(blk);
        }

        rp.setUsed(s, true);
        currSlot = s;
        flush();
        // NOTE:
        // ここではまだ列値がセットされていないため index 挿入はしない。
        // 値のセット時（setInt/setString）で旧値→新値差分に基づき更新する。
    }

    /**
     * 現在レコードを削除
     * トランザクション有効時：削除前に排他ロック（X-Lock）を取得
     */
    public void delete() {
        if (currSlot < 0)
            throw new IllegalStateException("no current record");

        // トランザクション有効時：排他ロックを取得
        if (tx != null) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireWriteLock(blk);
        }

        // 削除前にインデックスから当該レコードを取り除く（INT列のみ）
        if (indexMaintEnabled) {
            for (String fld : tf.layout().schema().fields().keySet()) {
                // INT列のみサポート
                if (tf.layout().schema().fieldType(fld) == FieldType.INT && hasIndexOn(fld)) {
                    int v = getInt(fld);
                    deleteIndexInt(fld, v);
                }
            }
        }
        rp.setUsed(currSlot, false);
        flush();
    }

    /**
     * 書き込みロックを取得するヘルパーメソッド
     */
    private void acquireWriteLock(BlockId blk) {
        if (tx == null)
            return;

        // Txを経由してロック取得（内部でxLockを実行）
        // ダミー書き込みでロックを取得
        tx.setInt(blk, 0, tx.getInt(blk, 0));
    }

    // フィールドI/O
    public int getInt(String fld) {
        // トランザクション有効時：読み取り前にロック取得
        if (tx != null && tx.getIsolationLevel().usesReadLocks()) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireReadLock(blk);
        }
        return rp.getInt(currSlot, fld);
    }

    /**
     * INT列の値を設定
     * トランザクション有効時：書き込み前に排他ロック（X-Lock）を取得
     */
    public void setInt(String fld, int v) {
        // トランザクション有効時：排他ロックを取得
        if (tx != null) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireWriteLock(blk);
        }

        int old = rp.getInt(currSlot, fld);
        rp.setInt(currSlot, fld, v);
        flush();
        if (indexMaintEnabled && tf.layout().schema().fieldType(fld) == FieldType.INT) {
            // 旧値→新値でB+木を更新
            updateIndexInt(fld, old, v);
        }
    }

    public String getString(String fld) {
        // トランザクション有効時：読み取り前にロック取得
        if (tx != null && tx.getIsolationLevel().usesReadLocks()) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireReadLock(blk);
        }
        return rp.getString(currSlot, fld);
    }

    /**
     * STRING列の値を設定
     * トランザクション有効時：書き込み前に排他ロック（X-Lock）を取得
     */
    public void setString(String fld, String v) {
        // トランザクション有効時：排他ロックを取得
        if (tx != null) {
            BlockId blk = new BlockId(tf.filename(), currBlk);
            acquireWriteLock(blk);
        }

        rp.setString(currSlot, fld, v);
        flush();
        // TODO(将来拡張): STRINGキーのB+木対応時にここで index 更新
    }

    /** 指定ブロックへページを読み込み、RecordPage を張り替える */
    private boolean moveToBlock(int blkNum) {
        if (blkNum < 0 || blkNum >= tf.size())
            return false;
        currBlk = blkNum;
        BlockId b = new BlockId(tf.filename(), currBlk);
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, tf.layout(), fm.blockSize());
        currSlot = -1;
        return true;
    }

    /** 新規ブロックを append して、そのブロックへ移動 */
    private void appendNewBlockAndMove() {
        BlockId b = tf.appendFormatted(); // ここで layout に従って空ページを初期化
        currBlk = b.number();
        Page p = new Page(fm.blockSize());
        fm.read(b, p);
        rp = new RecordPage(p, tf.layout(), fm.blockSize());
        currSlot = -1;
    }

    /** 現在ページをディスクへ書き戻す */
    private void flush() {
        if (rp == null || currBlk < 0)
            return;
        BlockId b = new BlockId(tf.filename(), currBlk);
        fm.write(b, rp.page()); // ← RecordPage に page() ゲッターを追加（下の最小パッチ参照）
    }

    // hasField
    public boolean hasField(String fldName) {
        return tf.hasField(fldName);
    }

    @Override
    public void close() {
        // 今の設計では明示 flush は各setterで実施済み。必要ならここで後始末を。
    }

    // デバッグ/確認用
    public int currentBlockNumber() {
        return currBlk;
    }

    public int currentSlot() {
        return currSlot;
    }

    // ---- index maintenance (optional) ----
    private boolean indexMaintEnabled = false;
    private String tableNameForIndex = null;
    private MetadataManager md = null;
    // column -> Optional<indexName>; present if the column has an index
    private java.util.Map<String, java.util.Optional<String>> indexByCol = new java.util.HashMap<>();

    /** ユーザーテーブルに対し、インデックス自動維持を有効化する */
    public TableScan enableIndexMaintenance(MetadataManager md, String tableName) {
        this.indexMaintEnabled = true;
        this.md = md;
        this.tableNameForIndex = tableName;
        this.indexByCol.clear();
        return this;
    }

    private boolean hasIndexOn(String col) {
        if (!indexMaintEnabled)
            return false;
        return indexByCol
                .computeIfAbsent(col, c -> md.findIndexOn(tableNameForIndex, c))
                .isPresent();
    }

    private String indexNameOf(String col) {
        return indexByCol.getOrDefault(col, java.util.Optional.empty()).orElse(null);
    }

    /** INT列の旧値→新値でB+木を更新（旧値==新値なら何もしない） */
    private void updateIndexInt(String col, int oldVal, int newVal) {
        if (!hasIndexOn(col))
            return;
        if (oldVal == newVal)
            return;
        String iname = indexNameOf(col);
        try (BTreeIndex ix = openIndex(iname)) {
            RID r = rid(); // 現在レコード
            // 旧値を消し、新値を入れる（旧値の存在しないINSERT直後でも安全）
            ix.delete(SearchKey.ofInt(oldVal), r);
            ix.insert(SearchKey.ofInt(newVal), r);
        }
    }

    /** INT列の単純DELETE */
    private void deleteIndexInt(String col, int val) {
        if (!hasIndexOn(col))
            return;
        String iname = indexNameOf(col);
        try (BTreeIndex ix = openIndex(iname)) {
            ix.delete(SearchKey.ofInt(val), rid());
        }
    }

    private BTreeIndex openIndex(String indexName) {
        try {
            BTreeIndex ix = new BTreeIndex(fm, indexName, tf.filename());
            ix.open();
            return ix;
        } catch (Exception e) {
            throw new RuntimeException("failed to open BTree index " + indexName, e);
        }
    }
}
