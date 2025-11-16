package app.tx.lock;

import app.storage.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 並行トランザクションのテスト。
 * Lost Update、Dirty Read などの異常現象が防止されることを検証します。
 */
@DisplayName("並行トランザクションのテスト")
class ConcurrencyTest {
    
    private LockTable lockTable;
    private BlockId testBlock;
    
    @BeforeEach
    void setUp() {
        lockTable = new LockTable(5000);  // 5秒のタイムアウト
        testBlock = new BlockId("test.tbl", 0);
    }
    
    @Test
    @DisplayName("Lost Update 防止: 2つのトランザクションが同時に更新しようとする")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testPreventLostUpdate() throws InterruptedException {
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);
        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch doneLatch = new CountDownLatch(2);
        
        // トランザクション1: 排他ロックを取得して更新
        Thread t1 = new Thread(() -> {
            try {
                startLatch.countDown();
                startLatch.await();  // 両方のスレッドが準備完了するまで待つ
                
                lockTable.xLock(testBlock, 1);
                Thread.sleep(100);  // 更新処理をシミュレート
                lockTable.unlock(testBlock, 1);
                
                successCount.incrementAndGet();
            } catch (Exception e) {
                failureCount.incrementAndGet();
            } finally {
                doneLatch.countDown();
            }
        });
        
        // トランザクション2: 排他ロックを取得して更新
        Thread t2 = new Thread(() -> {
            try {
                startLatch.countDown();
                startLatch.await();
                
                lockTable.xLock(testBlock, 2);
                Thread.sleep(100);  // 更新処理をシミュレート
                lockTable.unlock(testBlock, 2);
                
                successCount.incrementAndGet();
            } catch (Exception e) {
                failureCount.incrementAndGet();
            } finally {
                doneLatch.countDown();
            }
        });
        
        t1.start();
        t2.start();
        doneLatch.await();
        
        // 両方のトランザクションが成功するが、順次実行される
        assertEquals(2, successCount.get(), "両方のトランザクションが成功すべき");
        assertEquals(0, failureCount.get(), "失敗は発生しないべき");
    }
    
    @Test
    @DisplayName("Dirty Read 防止: 未コミットのデータを読み取らない")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testPreventDirtyRead() throws InterruptedException {
        // 短いタイムアウトのロックテーブルを使用
        LockTable shortTimeoutLockTable = new LockTable(500);  // 500msタイムアウト
        LockManager lockMgr1 = new LockManager(shortTimeoutLockTable);
        LockManager lockMgr2 = new LockManager(shortTimeoutLockTable);
        
        AtomicBoolean t2CanRead = new AtomicBoolean(false);
        CountDownLatch t1WriteLatch = new CountDownLatch(1);
        CountDownLatch t2ReadLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        
        // トランザクション1: 書き込んでからコミット前に待機
        Thread t1 = new Thread(() -> {
            try {
                lockMgr1.xLock(testBlock, 1);  // 排他ロック取得
                // 書き込み処理...
                t1WriteLatch.countDown();  // 書き込み完了を通知
                
                Thread.sleep(1000);  // コミット前に待機（タイムアウトより長い）
                
                lockMgr1.release(1);  // コミット（ロック解放）
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        // トランザクション2: トランザクション1のコミット前に読み取ろうとする
        Thread t2 = new Thread(() -> {
            try {
                t1WriteLatch.await();  // トランザクション1の書き込みを待つ
                Thread.sleep(50);  // トランザクション1がまだコミットしていない
                
                try {
                    lockMgr2.sLock(testBlock, 2);  // 共有ロック取得を試みる
                    t2CanRead.set(true);  // ロック取得に成功
                    lockMgr2.release(2);
                } catch (LockAbortException e) {
                    // タイムアウト（期待される）
                    t2CanRead.set(false);
                }
                
                t2ReadLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        t1.start();
        t2.start();
        
        t2ReadLatch.await();
        
        // トランザクション2は、トランザクション1がコミットするまで読み取れない
        assertFalse(t2CanRead.get(), "トランザクション2は未コミットのデータを読み取るべきではない");
        
        doneLatch.await();
    }
    
    @Test
    @DisplayName("Non-Repeatable Read: 共有ロックによる一貫性保証")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testRepeatableRead() throws InterruptedException {
        // 短いタイムアウトのロックテーブルを使用
        LockTable shortTimeoutLockTable = new LockTable(500);  // 500msタイムアウト
        LockManager lockMgr1 = new LockManager(shortTimeoutLockTable);
        LockManager lockMgr2 = new LockManager(shortTimeoutLockTable);
        
        AtomicBoolean t2CanWrite = new AtomicBoolean(false);
        CountDownLatch t1ReadLatch = new CountDownLatch(1);
        CountDownLatch t2DoneLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        
        // トランザクション1: 読み取ってから少し待つ
        Thread t1 = new Thread(() -> {
            try {
                lockMgr1.sLock(testBlock, 1);  // 共有ロック取得
                // 読み取り処理...
                t1ReadLatch.countDown();
                
                Thread.sleep(1000);  // 再読み取り前に待機（タイムアウトより長い）
                
                // 同じデータを再読み取り...
                lockMgr1.release(1);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        // トランザクション2: トランザクション1が読み取り中に更新しようとする
        Thread t2 = new Thread(() -> {
            try {
                t1ReadLatch.await();  // トランザクション1の読み取りを待つ
                Thread.sleep(50);
                
                try {
                    lockMgr2.xLock(testBlock, 2);  // 排他ロック取得を試みる
                    t2CanWrite.set(true);
                    lockMgr2.release(2);
                } catch (LockAbortException e) {
                    // タイムアウト（期待される）
                    t2CanWrite.set(false);
                }
                t2DoneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        t1.start();
        t2.start();
        t2DoneLatch.await();  // トランザクション2の結果を先に確認
        
        // トランザクション2は、トランザクション1が終了するまで書き込めない
        assertFalse(t2CanWrite.get(), "トランザクション2はトランザクション1の読み取り中に書き込むべきではない");
        
        doneLatch.await();
    }
    
    @Test
    @DisplayName("複数のトランザクションが同時に異なるブロックを操作できる")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testConcurrentAccessToDifferentBlocks() throws InterruptedException {
        BlockId block1 = new BlockId("test.tbl", 0);
        BlockId block2 = new BlockId("test.tbl", 1);
        BlockId block3 = new BlockId("test.tbl", 2);
        
        LockManager lockMgr1 = new LockManager(lockTable);
        LockManager lockMgr2 = new LockManager(lockTable);
        LockManager lockMgr3 = new LockManager(lockTable);
        
        AtomicInteger successCount = new AtomicInteger(0);
        CountDownLatch doneLatch = new CountDownLatch(3);
        
        Thread t1 = new Thread(() -> {
            try {
                lockMgr1.xLock(block1, 1);
                Thread.sleep(100);
                lockMgr1.release(1);
                successCount.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        Thread t2 = new Thread(() -> {
            try {
                lockMgr2.xLock(block2, 2);
                Thread.sleep(100);
                lockMgr2.release(2);
                successCount.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        Thread t3 = new Thread(() -> {
            try {
                lockMgr3.xLock(block3, 3);
                Thread.sleep(100);
                lockMgr3.release(3);
                successCount.incrementAndGet();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        t1.start();
        t2.start();
        t3.start();
        doneLatch.await();
        
        // 異なるブロックなので、3つすべてが並行実行できる
        assertEquals(3, successCount.get(), "すべてのトランザクションが成功すべき");
    }
    
    @Test
    @DisplayName("ロックアップグレード: 共有ロックから排他ロックへ")
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void testLockUpgrade() throws InterruptedException {
        // 短いタイムアウトのロックテーブルを使用
        LockTable shortTimeoutLockTable = new LockTable(500);  // 500msタイムアウト
        LockManager lockMgr1 = new LockManager(shortTimeoutLockTable);
        LockManager lockMgr2 = new LockManager(shortTimeoutLockTable);
        
        AtomicBoolean upgradeSuccess = new AtomicBoolean(false);
        AtomicBoolean t2Blocked = new AtomicBoolean(false);
        CountDownLatch t1UpgradeLatch = new CountDownLatch(1);
        CountDownLatch t2DoneLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);
        
        // トランザクション1: 共有ロックを取得してからアップグレード
        Thread t1 = new Thread(() -> {
            try {
                lockMgr1.sLock(testBlock, 1);  // 共有ロック
                Thread.sleep(50);
                
                lockMgr1.xLock(testBlock, 1);  // 排他ロックへアップグレード
                upgradeSuccess.set(true);
                t1UpgradeLatch.countDown();
                
                Thread.sleep(1000);  // タイムアウトより長く保持
                lockMgr1.release(1);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        // トランザクション2: トランザクション1がアップグレードした後にロック取得を試みる
        Thread t2 = new Thread(() -> {
            try {
                t1UpgradeLatch.await();  // トランザクション1のアップグレードを待つ
                Thread.sleep(50);
                
                try {
                    lockMgr2.xLock(testBlock, 2);
                    lockMgr2.release(2);
                } catch (LockAbortException e) {
                    t2Blocked.set(true);  // ブロックされた
                }
                t2DoneLatch.countDown();
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                doneLatch.countDown();
            }
        });
        
        t1.start();
        t2.start();
        t2DoneLatch.await();  // トランザクション2の結果を先に確認
        
        assertTrue(upgradeSuccess.get(), "ロックアップグレードが成功すべき");
        assertTrue(t2Blocked.get(), "トランザクション2はブロックされるべき");
        
        doneLatch.await();
    }
}
