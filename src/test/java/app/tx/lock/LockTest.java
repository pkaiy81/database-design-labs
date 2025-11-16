package app.tx.lock;

import app.storage.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Lock クラスの単体テスト。
 * 共有ロック、排他ロック、ロックアップグレード、タイムアウト処理などをテストします。
 */
@DisplayName("Lock クラスのテスト")
class LockTest {

    private Lock lock;
    private static final long SHORT_TIMEOUT = 100; // 100ms

    @BeforeEach
    void setUp() {
        lock = new Lock();
    }

    @Test
    @DisplayName("単一トランザクションが共有ロックを取得できる")
    void testSingleSharedLock() {
        assertDoesNotThrow(() -> lock.sLock(1, 1000));
        assertTrue(lock.isLockedBy(1));
    }

    @Test
    @DisplayName("単一トランザクションが排他ロックを取得できる")
    void testSingleExclusiveLock() {
        assertDoesNotThrow(() -> lock.xLock(1, 1000));
        assertTrue(lock.isLockedBy(1));
    }

    @Test
    @DisplayName("複数のトランザクションが共有ロックを同時に取得できる")
    void testMultipleSharedLocks() {
        assertDoesNotThrow(() -> {
            lock.sLock(1, 1000);
            lock.sLock(2, 1000);
            lock.sLock(3, 1000);
        });

        assertTrue(lock.isLockedBy(1));
        assertTrue(lock.isLockedBy(2));
        assertTrue(lock.isLockedBy(3));
    }

    @Test
    @DisplayName("排他ロックは共有ロックをブロックする")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    void testExclusiveLocksBlocksShared() throws InterruptedException {
        lock.xLock(1, 1000); // トランザクション1が排他ロックを取得

        AtomicBoolean blocked = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);

        // トランザクション2が共有ロックを試みる（ブロックされるはず）
        Thread t2 = new Thread(() -> {
            latch.countDown();
            try {
                lock.sLock(2, SHORT_TIMEOUT);
                blocked.set(false); // タイムアウトしなければここに到達
            } catch (LockAbortException e) {
                // タイムアウト期待
            }
        });
        t2.start();

        latch.await(); // スレッドが開始されるのを待つ
        Thread.sleep(200); // 十分にブロックされる時間を与える

        assertTrue(blocked.get(), "共有ロックは排他ロックによってブロックされるべき");

        t2.join();
    }

    @Test
    @DisplayName("共有ロックは排他ロックをブロックする")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    void testSharedLocksBlockExclusive() throws InterruptedException {
        lock.sLock(1, 1000); // トランザクション1が共有ロックを取得

        AtomicBoolean blocked = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(1);

        // トランザクション2が排他ロックを試みる（ブロックされるはず）
        Thread t2 = new Thread(() -> {
            latch.countDown();
            try {
                lock.xLock(2, SHORT_TIMEOUT);
                blocked.set(false); // タイムアウトしなければここに到達
            } catch (LockAbortException e) {
                // タイムアウト期待
            }
        });
        t2.start();

        latch.await();
        Thread.sleep(200);

        assertTrue(blocked.get(), "排他ロックは共有ロックによってブロックされるべき");

        t2.join();
    }

    @Test
    @DisplayName("ロック解放後に他のトランザクションが取得できる")
    @Timeout(value = 3, unit = TimeUnit.SECONDS)
    void testLockReleaseAllowsOthers() throws InterruptedException {
        lock.xLock(1, 1000);

        AtomicBoolean acquired = new AtomicBoolean(false);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(1);

        Thread t2 = new Thread(() -> {
            startLatch.countDown();
            try {
                lock.xLock(2, 2000);
                acquired.set(true);
            } catch (LockAbortException e) {
                // タイムアウト
            }
            doneLatch.countDown();
        });
        t2.start();

        startLatch.await();
        Thread.sleep(100); // t2がブロックされる時間を与える

        lock.unlock(1); // トランザクション1がロックを解放

        doneLatch.await();
        assertTrue(acquired.get(), "ロック解放後に他のトランザクションが取得できるべき");

        t2.join();
    }

    @Test
    @DisplayName("共有ロックから排他ロックへのアップグレード")
    void testLockUpgrade() {
        lock.sLock(1, 1000);
        assertTrue(lock.isLockedBy(1));

        // アップグレード（他に共有ロック保持者がいない）
        assertDoesNotThrow(() -> lock.xLock(1, 1000));
        assertTrue(lock.isLockedBy(1));
    }

    @Test
    @DisplayName("同じトランザクションが同じロックを複数回取得できる（冪等性）")
    void testIdempotentLocking() {
        // 共有ロックの冪等性
        assertDoesNotThrow(() -> {
            lock.sLock(1, 1000);
            lock.sLock(1, 1000); // 2回目
            lock.sLock(1, 1000); // 3回目
        });

        lock.unlock(1); // トランザクション1のロックを解放

        // 排他ロックの冪等性
        assertDoesNotThrow(() -> {
            lock.xLock(2, 1000);
            lock.xLock(2, 1000); // 2回目
        });
    }

    @Test
    @DisplayName("ロックタイムアウトが正しく動作する")
    void testLockTimeout() {
        lock.xLock(1, 1000);

        // トランザクション2がタイムアウトするはず
        LockAbortException exception = assertThrows(
                LockAbortException.class,
                () -> lock.xLock(2, SHORT_TIMEOUT));

        assertTrue(exception.getMessage().contains("タイムアウト"));
    }

    @Test
    @DisplayName("toString() がロック状態を正しく表示する")
    void testToString() {
        String unlocked = lock.toString();
        assertTrue(unlocked.contains("Unlocked"));

        lock.sLock(1, 1000);
        String sLocked = lock.toString();
        assertTrue(sLocked.contains("S-Lock"));
        assertTrue(sLocked.contains("Tx"));

        lock.unlock(1);
        lock.xLock(2, 1000);
        String xLocked = lock.toString();
        assertTrue(xLocked.contains("X-Lock"));
        assertTrue(xLocked.contains("Tx2"));
    }
}
