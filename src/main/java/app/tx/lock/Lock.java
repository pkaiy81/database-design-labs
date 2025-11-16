package app.tx.lock;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 単一のデータブロックに対するロックを管理するクラス。
 * 
 * <p>このクラスは、特定のブロック（BlockId）に対する共有ロック（SHARED）と
 * 排他ロック（EXCLUSIVE）の取得・解放を管理します。
 * 
 * <h3>ロックの取得ルール</h3>
 * <ul>
 *   <li><b>共有ロック</b>: 複数のトランザクションが同時に取得可能（排他ロックがない場合のみ）</li>
 *   <li><b>排他ロック</b>: 1つのトランザクションのみが取得可能（他のロックが一切ない場合のみ）</li>
 * </ul>
 * 
 * <h3>ロックアップグレード</h3>
 * <p>共有ロックを持っているトランザクションが排他ロックを要求した場合、
 * 他に共有ロックを持つトランザクションがいなければ、アップグレードが許可されます。
 * 
 * <h3>スレッドセーフティ</h3>
 * <p>このクラスはスレッドセーフです。内部で {@link ReentrantLock} を使用して
 * 複数のスレッドからの同時アクセスを制御しています。
 * 
 * @see LockType
 * @see LockTable
 * @see LockManager
 */
public class Lock {
    /** 共有ロックを保持しているトランザクションIDのセット */
    private final Set<Integer> sharedHolders;
    
    /** 排他ロックを保持しているトランザクションID（-1は未保持を示す） */
    private int exclusiveHolder;
    
    /** このロックへのアクセスを制御する内部ロック */
    private final ReentrantLock lock;
    
    /** ロック待機中のスレッドに通知するための条件変数 */
    private final Condition condition;
    
    /** ロック要求の待機キュー（公平性を保証） */
    private final Queue<LockRequest> waitQueue;
    
    /**
     * ロック要求を表す内部クラス。
     * 待機キューに格納され、FIFO順で処理されます。
     */
    private static class LockRequest {
        final int txNum;
        final LockType lockType;
        final long requestTime;
        
        LockRequest(int txNum, LockType lockType) {
            this.txNum = txNum;
            this.lockType = lockType;
            this.requestTime = System.currentTimeMillis();
        }
    }
    
    /**
     * 新しいロックオブジェクトを作成します。
     * 初期状態では、どのトランザクションもロックを保持していません。
     */
    public Lock() {
        this.sharedHolders = new HashSet<>();
        this.exclusiveHolder = -1;
        this.lock = new ReentrantLock(true);  // 公平なロック
        this.condition = lock.newCondition();
        this.waitQueue = new LinkedList<>();
    }
    
    /**
     * 指定されたトランザクションが共有ロックを取得します。
     * 
     * <p>ロックを取得できるまで待機します。タイムアウトした場合は例外をスローします。
     * 
     * @param txNum トランザクション番号
     * @param timeoutMs タイムアウト時間（ミリ秒）
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void sLock(int txNum, long timeoutMs) {
        lock.lock();
        try {
            // すでに共有ロックまたは排他ロックを持っている場合は即座に成功
            if (sharedHolders.contains(txNum) || exclusiveHolder == txNum) {
                return;
            }
            
            // 待機キューに追加
            LockRequest request = new LockRequest(txNum, LockType.SHARED);
            waitQueue.add(request);
            
            long deadline = System.currentTimeMillis() + timeoutMs;
            
            // ロックを取得できるまで待機
            while (!canAcquireShared(txNum)) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    waitQueue.remove(request);
                    throw new LockAbortException(
                        "タイムアウト: 共有ロックを取得できませんでした (txNum=" + txNum + ", timeout=" + timeoutMs + "ms)"
                    );
                }
                
                try {
                    condition.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    waitQueue.remove(request);
                    Thread.currentThread().interrupt();
                    throw new LockAbortException("ロック待機中に割り込まれました (txNum=" + txNum + ")", e);
                }
            }
            
            // ロックを取得
            waitQueue.remove(request);
            sharedHolders.add(txNum);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 指定されたトランザクションが排他ロックを取得します。
     * 
     * <p>ロックを取得できるまで待機します。タイムアウトした場合は例外をスローします。
     * 
     * @param txNum トランザクション番号
     * @param timeoutMs タイムアウト時間（ミリ秒）
     * @throws LockAbortException ロックの取得に失敗した場合
     */
    public void xLock(int txNum, long timeoutMs) {
        lock.lock();
        try {
            // すでに排他ロックを持っている場合は即座に成功
            if (exclusiveHolder == txNum) {
                return;
            }
            
            // 待機キューに追加
            LockRequest request = new LockRequest(txNum, LockType.EXCLUSIVE);
            waitQueue.add(request);
            
            long deadline = System.currentTimeMillis() + timeoutMs;
            
            // ロックを取得できるまで待機
            while (!canAcquireExclusive(txNum)) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    waitQueue.remove(request);
                    throw new LockAbortException(
                        "タイムアウト: 排他ロックを取得できませんでした (txNum=" + txNum + ", timeout=" + timeoutMs + "ms)"
                    );
                }
                
                try {
                    condition.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    waitQueue.remove(request);
                    Thread.currentThread().interrupt();
                    throw new LockAbortException("ロック待機中に割り込まれました (txNum=" + txNum + ")", e);
                }
            }
            
            // ロックを取得（アップグレードの場合は共有ロックを解放）
            waitQueue.remove(request);
            if (sharedHolders.contains(txNum)) {
                sharedHolders.remove(txNum);
            }
            exclusiveHolder = txNum;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 指定されたトランザクションが保持しているロックを解放します。
     * 
     * @param txNum トランザクション番号
     */
    public void unlock(int txNum) {
        lock.lock();
        try {
            boolean wasLocked = false;
            
            // 共有ロックを解放
            if (sharedHolders.contains(txNum)) {
                sharedHolders.remove(txNum);
                wasLocked = true;
            }
            
            // 排他ロックを解放
            if (exclusiveHolder == txNum) {
                exclusiveHolder = -1;
                wasLocked = true;
            }
            
            // ロックを解放した場合は待機中のスレッドに通知
            if (wasLocked) {
                condition.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 指定されたトランザクションが共有ロックを取得可能か判定します。
     * 
     * @param txNum トランザクション番号
     * @return 取得可能な場合は true
     */
    private boolean canAcquireShared(int txNum) {
        // すでに共有ロックまたは排他ロックを持っている
        if (sharedHolders.contains(txNum) || exclusiveHolder == txNum) {
            return true;
        }
        
        // 排他ロックを持っているトランザクションがいない
        if (exclusiveHolder == -1) {
            // キューの先頭が自分か、または共有ロック要求である
            LockRequest first = waitQueue.peek();
            if (first != null && (first.txNum == txNum || first.lockType == LockType.SHARED)) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * 指定されたトランザクションが排他ロックを取得可能か判定します。
     * 
     * @param txNum トランザクション番号
     * @return 取得可能な場合は true
     */
    private boolean canAcquireExclusive(int txNum) {
        // すでに排他ロックを持っている
        if (exclusiveHolder == txNum) {
            return true;
        }
        
        // 他のトランザクションが排他ロックを持っている
        if (exclusiveHolder != -1 && exclusiveHolder != txNum) {
            return false;
        }
        
        // 自分以外が共有ロックを持っている
        if (!sharedHolders.isEmpty()) {
            if (sharedHolders.size() == 1 && sharedHolders.contains(txNum)) {
                // 自分だけが共有ロックを持っている場合はアップグレード可能
                LockRequest first = waitQueue.peek();
                return first != null && first.txNum == txNum;
            }
            return false;
        }
        
        // キューの先頭が自分である
        LockRequest first = waitQueue.peek();
        return first != null && first.txNum == txNum;
    }
    
    /**
     * このロックの現在の状態を文字列で返します（デバッグ用）。
     * 
     * @return ロックの状態を示す文字列
     */
    @Override
    public String toString() {
        lock.lock();
        try {
            StringBuilder sb = new StringBuilder();
            sb.append("Lock[");
            if (exclusiveHolder != -1) {
                sb.append("X-Lock by Tx").append(exclusiveHolder);
            } else if (!sharedHolders.isEmpty()) {
                sb.append("S-Lock by Tx").append(sharedHolders);
            } else {
                sb.append("Unlocked");
            }
            if (!waitQueue.isEmpty()) {
                sb.append(", Waiting: ").append(waitQueue.size());
            }
            sb.append("]");
            return sb.toString();
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * 指定されたトランザクションがこのロックを保持しているか判定します。
     * 
     * @param txNum トランザクション番号
     * @return ロックを保持している場合は true
     */
    public boolean isLockedBy(int txNum) {
        lock.lock();
        try {
            return sharedHolders.contains(txNum) || exclusiveHolder == txNum;
        } finally {
            lock.unlock();
        }
    }
}
