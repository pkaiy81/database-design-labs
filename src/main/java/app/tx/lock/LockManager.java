package app.tx.lock;

import app.storage.BlockId;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * トランザクション毎のロック管理を行うクラス。
 * 
 * <p>
 * 各トランザクションは独自の {@link LockManager} インスタンスを持ち、
 * そのトランザクションが取得したすべてのロックを追跡します。
 * トランザクションの終了時（コミットまたはロールバック）に、
 * すべてのロックを自動的に解放します。
 * 
 * <h3>2PL (Two-Phase Locking) プロトコル</h3>
 * <p>
 * このクラスは、厳格な2相ロック（Strict 2PL）プロトコルを実装しています：
 * <ul>
 * <li><b>Growing Phase（拡張フェーズ）</b>: ロックを取得できるが、解放はできない</li>
 * <li><b>Shrinking Phase（縮小フェーズ）</b>: すべてのロックを一度に解放（トランザクション終了時）</li>
 * </ul>
 * 
 * <h3>使用例</h3>
 * 
 * <pre>{@code
 * LockTable lockTable = new LockTable();
 * LockManager lockMgr = new LockManager(lockTable);
 * 
 * BlockId blk = new BlockId("test.tbl", 0);
 * 
 * // ロックを取得
 * lockMgr.sLock(blk, 1); // 共有ロック
 * lockMgr.xLock(blk, 1); // 排他ロック（アップグレード）
 * 
 * // トランザクション終了時にすべてのロックを解放
 * lockMgr.release(1);
 * }</pre>
 * 
 * @see LockTable
 * @see Lock
 */
public class LockManager {
    /** グローバルなロックテーブル */
    private final LockTable lockTable;

    /** トランザクション毎に保持しているロックのマップ (txNum -> ロックされたブロックのセット) */
    private final Map<Integer, Set<BlockId>> locks;

    /**
     * 指定されたロックテーブルを使用してロックマネージャーを作成します。
     * 
     * @param lockTable グローバルなロックテーブル
     */
    public LockManager(LockTable lockTable) {
        this.lockTable = lockTable;
        this.locks = new HashMap<>();
    }

    /**
     * 指定されたブロックに対して共有ロックを取得します。
     * 
     * <p>
     * ロックが取得されると、トランザクションのロックセットに記録されます。
     * すでに同じブロックに対してロックを持っている場合は、何もしません。
     * 
     * @param blk   ロック対象のブロック
     * @param txNum トランザクション番号
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void sLock(BlockId blk, int txNum) {
        // すでにこのブロックをロックしている場合はスキップ
        if (hasLock(blk, txNum)) {
            return;
        }

        // グローバルロックテーブルから共有ロックを取得
        lockTable.sLock(blk, txNum);

        // ロックセットに追加
        locks.computeIfAbsent(txNum, k -> new HashSet<>()).add(blk);
    }

    /**
     * 指定されたブロックに対して排他ロックを取得します。
     * 
     * <p>
     * ロックが取得されると、トランザクションのロックセットに記録されます。
     * すでに同じブロックに対してロックを持っている場合でも、
     * 排他ロックへのアップグレードを試みます。
     * 
     * @param blk   ロック対象のブロック
     * @param txNum トランザクション番号
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void xLock(BlockId blk, int txNum) {
        // グローバルロックテーブルから排他ロックを取得
        lockTable.xLock(blk, txNum);

        // ロックセットに追加（すでに共有ロックを持っている場合もある）
        locks.computeIfAbsent(txNum, k -> new HashSet<>()).add(blk);
    }

    /**
     * 指定されたブロックに対するロックを解放します。
     * 
     * <p>
     * このメソッドは、READ_COMMITTED分離レベルで読み取り直後にロックを解放する際に使用されます。
     * 通常のStrict 2PLでは、トランザクション終了時まですべてのロックを保持するため、
     * このメソッドは慎重に使用する必要があります。
     * 
     * @param blk   ロック対象のブロック
     * @param txNum トランザクション番号
     */
    public void unlock(BlockId blk, int txNum) {
        Set<BlockId> lockedBlocks = locks.get(txNum);
        if (lockedBlocks != null && lockedBlocks.contains(blk)) {
            lockTable.unlock(blk, txNum);
            lockedBlocks.remove(blk);
            if (lockedBlocks.isEmpty()) {
                locks.remove(txNum);
            }
        }
    }

    /**
     * 指定されたトランザクションが保持しているすべてのロックを解放します。
     * 
     * <p>
     * このメソッドは、トランザクションのコミットまたはロールバック時に
     * 呼び出され、Strict 2PL プロトコルを実現します。
     * 
     * @param txNum トランザクション番号
     */
    public void release(int txNum) {
        Set<BlockId> lockedBlocks = locks.get(txNum);
        if (lockedBlocks != null) {
            // すべてのロックを解放
            for (BlockId blk : lockedBlocks) {
                lockTable.unlock(blk, txNum);
            }
            // ロックセットをクリア
            lockedBlocks.clear();
            locks.remove(txNum);
        }
    }

    /**
     * 指定されたトランザクションが指定されたブロックに対してロックを持っているか判定します。
     * 
     * @param blk   ブロックID
     * @param txNum トランザクション番号
     * @return ロックを持っている場合は true
     */
    public boolean hasLock(BlockId blk, int txNum) {
        Set<BlockId> lockedBlocks = locks.get(txNum);
        return lockedBlocks != null && lockedBlocks.contains(blk);
    }

    /**
     * 指定されたトランザクションが保持しているロックの数を返します。
     * 
     * @param txNum トランザクション番号
     * @return ロック数
     */
    public int getLockCount(int txNum) {
        Set<BlockId> lockedBlocks = locks.get(txNum);
        return lockedBlocks != null ? lockedBlocks.size() : 0;
    }

    /**
     * 現在のロックマネージャーの状態を文字列で返します（デバッグ用）。
     * 
     * @return ロックマネージャーの状態を示す文字列
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LockManager[\n");
        locks.forEach((txNum, blocksSet) -> {
            sb.append("  Tx").append(txNum).append(": ");
            sb.append(blocksSet.size()).append(" locks on ");
            sb.append(blocksSet);
            sb.append("\n");
        });
        sb.append("]");
        return sb.toString();
    }

    /**
     * 管理しているトランザクションの数を返します。
     * 
     * @return トランザクション数
     */
    public int getTransactionCount() {
        return locks.size();
    }

    /**
     * 指定されたトランザクションが保持しているすべてのブロックIDのセットを返します。
     * 
     * @param txNum トランザクション番号
     * @return ロックされているブロックIDのセット（読み取り専用）
     */
    public Set<BlockId> getLockedBlocks(int txNum) {
        Set<BlockId> lockedBlocks = locks.get(txNum);
        if (lockedBlocks == null) {
            return Set.of();
        }
        return Set.copyOf(lockedBlocks); // 防御的コピー
    }
}
