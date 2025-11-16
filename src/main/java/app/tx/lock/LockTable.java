package app.tx.lock;

import app.storage.BlockId;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

/**
 * すべてのデータブロックに対するロックを管理するグローバルなロックテーブル。
 * 
 * <p>このクラスは、データベース全体で単一のインスタンスとして使用され、
 * すべてのブロックに対するロックオブジェクトを管理します。
 * 
 * <h3>スレッドセーフティ</h3>
 * <p>このクラスはスレッドセーフです。内部で {@link ConcurrentHashMap} を使用して
 * 複数のスレッドからの同時アクセスに対応しています。
 * 
 * <h3>使用例</h3>
 * <pre>{@code
 * LockTable lockTable = new LockTable(10000); // タイムアウト10秒
 * BlockId blk = new BlockId("test.tbl", 0);
 * 
 * // 共有ロックを取得
 * lockTable.sLock(blk, 1);  // トランザクション1
 * 
 * // 排他ロックを取得
 * lockTable.xLock(blk, 2);  // トランザクション2（待機）
 * 
 * // ロックを解放
 * lockTable.unlock(blk, 1);  // トランザクション1が解放するとトランザクション2が取得
 * }</pre>
 * 
 * @see Lock
 * @see LockManager
 * @see BlockId
 */
public class LockTable {
    /** デフォルトのロックタイムアウト時間（ミリ秒）: 10秒 */
    private static final long DEFAULT_TIMEOUT_MS = 10000;
    
    /** ブロックIDからロックオブジェクトへのマッピング */
    private final Map<BlockId, Lock> locks;
    
    /** ロック取得時のタイムアウト時間（ミリ秒） */
    private final long timeoutMs;
    
    /**
     * デフォルトのタイムアウト時間（10秒）でロックテーブルを作成します。
     */
    public LockTable() {
        this(DEFAULT_TIMEOUT_MS);
    }
    
    /**
     * 指定されたタイムアウト時間でロックテーブルを作成します。
     * 
     * @param timeoutMs ロック取得時のタイムアウト時間（ミリ秒）
     */
    public LockTable(long timeoutMs) {
        this.locks = new ConcurrentHashMap<>();
        this.timeoutMs = timeoutMs;
    }
    
    /**
     * 指定されたブロックに対して共有ロックを取得します。
     * 
     * <p>ロックを取得できるまで待機します。タイムアウトした場合は
     * {@link LockAbortException} をスローします。
     * 
     * @param blk ロック対象のブロック
     * @param txNum トランザクション番号
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void sLock(BlockId blk, int txNum) {
        Lock lock = getLock(blk);
        lock.sLock(txNum, timeoutMs);
    }
    
    /**
     * 指定されたブロックに対して排他ロックを取得します。
     * 
     * <p>ロックを取得できるまで待機します。タイムアウトした場合は
     * {@link LockAbortException} をスローします。
     * 
     * @param blk ロック対象のブロック
     * @param txNum トランザクション番号
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void xLock(BlockId blk, int txNum) {
        Lock lock = getLock(blk);
        lock.xLock(txNum, timeoutMs);
    }
    
    /**
     * 指定されたブロックに対するロックを解放します。
     * 
     * @param blk ロック対象のブロック
     * @param txNum トランザクション番号
     */
    public void unlock(BlockId blk, int txNum) {
        Lock lock = locks.get(blk);
        if (lock != null) {
            lock.unlock(txNum);
        }
    }
    
    /**
     * 指定されたブロックに対応するロックオブジェクトを取得します。
     * 
     * <p>ロックオブジェクトが存在しない場合は新規作成します。
     * 
     * @param blk ブロックID
     * @return ロックオブジェクト
     */
    private Lock getLock(BlockId blk) {
        return locks.computeIfAbsent(blk, k -> new Lock());
    }
    
    /**
     * 現在のロックテーブルの状態を文字列で返します（デバッグ用）。
     * 
     * @return ロックテーブルの状態を示す文字列
     */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LockTable[timeout=").append(timeoutMs).append("ms, locks=").append(locks.size()).append("]\n");
        locks.forEach((blk, lock) -> {
            sb.append("  ").append(blk).append(": ").append(lock).append("\n");
        });
        return sb.toString();
    }
    
    /**
     * ロックテーブル内のロック数を返します。
     * 
     * @return ロック数
     */
    public int size() {
        return locks.size();
    }
    
    /**
     * 指定されたブロックがロックされているか判定します。
     * 
     * @param blk ブロックID
     * @param txNum トランザクション番号
     * @return ロックされている場合は true
     */
    public boolean isLocked(BlockId blk, int txNum) {
        Lock lock = locks.get(blk);
        return lock != null && lock.isLockedBy(txNum);
    }
}
