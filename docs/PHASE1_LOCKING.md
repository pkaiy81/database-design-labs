# Phase 1: 並行制御の完全実装

## 概要

このドキュメントは **Phase 1: 並行制御の完全実装** の詳細な実装ガイドです。

**目標**: マルチスレッド環境でのデータ整合性を保証する  
**期間**: 2-3週間  
**難易度**: ⭐⭐⭐ (中〜高)

---

## 📋 チェックリスト

### Week 1: ロック管理基盤

- [ ] `LockType` enum 作成
- [ ] `Lock` クラス作成
- [ ] `LockTable` クラス作成（グローバルロックテーブル）
- [ ] `LockManager` クラス作成（トランザクション毎）
- [ ] 基本的な shared/exclusive ロック取得・解放
- [ ] ユニットテスト作成
- [ ] `Tx` クラスへの統合

### Week 2: デッドロック検出と分離レベル

- [ ] Wait-For Graph 実装
- [ ] `DeadlockDetector` クラス作成
- [ ] タイムアウト機構実装
- [ ] `IsolationLevel` enum 作成
- [ ] 各分離レベルの実装
- [ ] 統合テスト作成

### Week 3: 統合とテスト

- [ ] `TableScan` へのロック統合
- [ ] 並行トランザクションテスト
- [ ] パフォーマンステスト
- [ ] ドキュメント作成
- [ ] コードレビュー

---

## 🏗️ アーキテクチャ

### クラス図

```
┌─────────────────────────────────────────────────────────────┐
│                         Tx                                   │
│  + commit()                                                  │
│  + rollback()                                                │
│  + setInt(BlockId, offset, value)                           │
│  + getString(BlockId, offset)                               │
└───────────────┬─────────────────────────────────────────────┘
                │ uses
                ▼
┌─────────────────────────────────────────────────────────────┐
│                    LockManager                               │
│  - txId: int                                                 │
│  - locks: Map<BlockId, LockType>                            │
│  + sLock(BlockId)                                            │
│  + xLock(BlockId)                                            │
│  + unlock(BlockId)                                           │
│  + unlockAll()                                               │
└───────────────┬─────────────────────────────────────────────┘
                │ uses
                ▼
┌─────────────────────────────────────────────────────────────┐
│                    LockTable                                 │
│  - locks: Map<BlockId, Lock>                                │
│  + sLock(BlockId, txId): boolean                            │
│  + xLock(BlockId, txId): boolean                            │
│  + unlock(BlockId, txId)                                     │
└───────────────┬─────────────────────────────────────────────┘
                │ contains
                ▼
┌─────────────────────────────────────────────────────────────┐
│                       Lock                                   │
│  - holders: Set<Integer>        // Shared lock holders      │
│  - exclusiveHolder: Integer     // Exclusive lock holder    │
│  - waiters: Queue<Integer>      // Waiting transactions     │
│  + addSharedHolder(txId)                                     │
│  + setExclusiveHolder(txId)                                  │
│  + release(txId)                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## 📝 実装詳細

### 1. LockType enum

```java
// src/main/java/app/tx/lock/LockType.java
package app.tx.lock;

public enum LockType {
    SHARED,      // 読み取りロック（複数トランザクション可）
    EXCLUSIVE    // 書き込みロック（排他的）
}
```

**説明**:

- `SHARED`: 読み取り専用。複数のトランザクションが同時に取得可能
- `EXCLUSIVE`: 書き込み用。1つのトランザクションのみが取得可能

---

### 2. Lock クラス

```java
// src/main/java/app/tx/lock/Lock.java
package app.tx.lock;

import java.util.*;
import java.util.concurrent.locks.*;

/**
 * 個別リソース（BlockId）に対するロックを表現。
 * Shared/Exclusive モードを管理し、待機キューを保持。
 */
public class Lock {
    private final Set<Integer> sharedHolders = new HashSet<>();
    private Integer exclusiveHolder = null;
    private final Queue<WaitEntry> waiters = new LinkedList<>();
    private final ReentrantLock mutex = new ReentrantLock();
    private final Condition condition = mutex.newCondition();

    /**
     * Shared lock を取得試行。
     * Exclusive lock が存在する場合は待機。
     */
    public boolean acquireShared(int txId, long timeoutMs) throws InterruptedException {
        mutex.lock();
        try {
            // 既に Shared または Exclusive を持っている場合は成功
            if (sharedHolders.contains(txId) || 
                (exclusiveHolder != null && exclusiveHolder == txId)) {
                return true;
            }

            long deadline = System.currentTimeMillis() + timeoutMs;
            
            // Exclusive holder がいる間は待機
            while (exclusiveHolder != null && exclusiveHolder != txId) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false; // タイムアウト
                }
                condition.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            sharedHolders.add(txId);
            return true;
        } finally {
            mutex.unlock();
        }
    }

    /**
     * Exclusive lock を取得試行。
     * 他のトランザクションがロックを保持している場合は待機。
     */
    public boolean acquireExclusive(int txId, long timeoutMs) throws InterruptedException {
        mutex.lock();
        try {
            // 既に Exclusive を持っている場合は成功
            if (exclusiveHolder != null && exclusiveHolder == txId) {
                return true;
            }

            long deadline = System.currentTimeMillis() + timeoutMs;

            // 他のトランザクションがロックを持っている間は待機
            while (exclusiveHolder != null || 
                   (!sharedHolders.isEmpty() && 
                    !(sharedHolders.size() == 1 && sharedHolders.contains(txId)))) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false; // タイムアウト
                }
                condition.await(remaining, java.util.concurrent.TimeUnit.MILLISECONDS);
            }

            // 自分の Shared lock を Exclusive にアップグレード
            sharedHolders.remove(txId);
            exclusiveHolder = txId;
            return true;
        } finally {
            mutex.unlock();
        }
    }

    /**
     * ロックを解放し、待機中のトランザクションに通知。
     */
    public void release(int txId) {
        mutex.lock();
        try {
            sharedHolders.remove(txId);
            if (exclusiveHolder != null && exclusiveHolder == txId) {
                exclusiveHolder = null;
            }
            condition.signalAll();
        } finally {
            mutex.unlock();
        }
    }

    /**
     * このロックを保持しているか確認。
     */
    public boolean isHeldBy(int txId) {
        mutex.lock();
        try {
            return sharedHolders.contains(txId) || 
                   (exclusiveHolder != null && exclusiveHolder == txId);
        } finally {
            mutex.unlock();
        }
    }

    private static class WaitEntry {
        final int txId;
        final LockType type;

        WaitEntry(int txId, LockType type) {
            this.txId = txId;
            this.type = type;
        }
    }
}
```

---

### 3. LockTable クラス

```java
// src/main/java/app/tx/lock/LockTable.java
package app.tx.lock;

import app.storage.BlockId;
import java.util.concurrent.ConcurrentHashMap;

/**
 * グローバルなロックテーブル。
 * すべてのBlockIdに対するロックを管理。
 */
public class LockTable {
    private static final long DEFAULT_TIMEOUT_MS = 10000; // 10秒

    private final ConcurrentHashMap<BlockId, Lock> locks = new ConcurrentHashMap<>();

    /**
     * Shared lock を取得。
     * @return true if successful, false if timeout
     */
    public boolean sLock(BlockId blk, int txId) {
        try {
            Lock lock = locks.computeIfAbsent(blk, k -> new Lock());
            return lock.acquireShared(txId, DEFAULT_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * Exclusive lock を取得。
     * @return true if successful, false if timeout
     */
    public boolean xLock(BlockId blk, int txId) {
        try {
            Lock lock = locks.computeIfAbsent(blk, k -> new Lock());
            return lock.acquireExclusive(txId, DEFAULT_TIMEOUT_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * ロックを解放。
     */
    public void unlock(BlockId blk, int txId) {
        Lock lock = locks.get(blk);
        if (lock != null) {
            lock.release(txId);
        }
    }
}
```

---

### 4. LockManager クラス

```java
// src/main/java/app/tx/lock/LockManager.java
package app.tx.lock;

import app.storage.BlockId;
import java.util.*;

/**
 * トランザクション毎のロック管理。
 * 取得したロックを追跡し、トランザクション終了時に一括解放。
 */
public class LockManager {
    private final int txId;
    private final LockTable lockTable;
    private final Map<BlockId, LockType> heldLocks = new HashMap<>();

    public LockManager(int txId, LockTable lockTable) {
        this.txId = txId;
        this.lockTable = lockTable;
    }

    /**
     * Shared lock を取得（必要に応じて）。
     */
    public void sLock(BlockId blk) {
        if (heldLocks.containsKey(blk)) {
            // 既にロックを保持している
            return;
        }

        if (!lockTable.sLock(blk, txId)) {
            throw new RuntimeException("Could not acquire shared lock on " + blk + " for tx " + txId);
        }

        heldLocks.put(blk, LockType.SHARED);
    }

    /**
     * Exclusive lock を取得（必要に応じてアップグレード）。
     */
    public void xLock(BlockId blk) {
        LockType current = heldLocks.get(blk);
        
        if (current == LockType.EXCLUSIVE) {
            // 既に Exclusive lock を保持
            return;
        }

        if (!lockTable.xLock(blk, txId)) {
            throw new RuntimeException("Could not acquire exclusive lock on " + blk + " for tx " + txId);
        }

        heldLocks.put(blk, LockType.EXCLUSIVE);
    }

    /**
     * 特定のロックを解放。
     */
    public void unlock(BlockId blk) {
        if (heldLocks.remove(blk) != null) {
            lockTable.unlock(blk, txId);
        }
    }

    /**
     * すべてのロックを解放（commit/rollback時）。
     */
    public void unlockAll() {
        for (BlockId blk : heldLocks.keySet()) {
            lockTable.unlock(blk, txId);
        }
        heldLocks.clear();
    }
}
```

---

### 5. Tx クラスへの統合

```java
// src/main/java/app/tx/Tx.java (変更)
package app.tx;

import app.memory.Buffer;
import app.memory.BufferMgr;
import app.memory.LogManager;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.storage.Page;
import app.tx.lock.LockManager;
import app.tx.lock.LockTable;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

public final class Tx implements AutoCloseable {
    private static final AtomicInteger SEQ = new AtomicInteger(1);
    private static final LockTable LOCK_TABLE = new LockTable(); // グローバル

    private final int txId;
    private final FileMgr fm;
    private final BufferMgr bm;
    private final LogManager log;
    private final Path logDir;
    private final LockManager lockMgr;

    public Tx(FileMgr fm, BufferMgr bm, LogManager log, Path logDir) {
        this.txId = SEQ.getAndIncrement();
        this.fm = fm;
        this.bm = bm;
        this.log = log;
        this.logDir = logDir;
        this.lockMgr = new LockManager(txId, LOCK_TABLE);
        
        // START ログ
        log.append(LogCodec.start(txId));
    }

    public int id() {
        return txId;
    }

    /**
     * 整数値を読み取る（Shared lock 取得）。
     */
    public int getInt(BlockId blk, int offset) {
        lockMgr.sLock(blk);  // ← Shared lock 取得
        Buffer buf = bm.pin(blk);
        try {
            return buf.contents().getInt(offset);
        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * 文字列を読み取る（Shared lock 取得）。
     */
    public String getString(BlockId blk, int offset) {
        lockMgr.sLock(blk);  // ← Shared lock 取得
        Buffer buf = bm.pin(blk);
        try {
            return buf.contents().getString(offset);
        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * 整数値を書き込む（Exclusive lock 取得、WAL）。
     */
    public void setInt(BlockId blk, int offset, int newVal) {
        lockMgr.xLock(blk);  // ← Exclusive lock 取得
        
        Buffer buf = bm.pin(blk);
        try {
            Page p = buf.contents();
            int old = p.getInt(offset);

            // 1) WAL: 旧値をログへ
            log.append(LogCodec.setInt(txId, blk.filename(), blk.number(), offset, old));
            log.flush(0);

            // 2) ページ更新
            p.setInt(offset, newVal);
            buf.setDirty();
            buf.flushIfDirty();
        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * 文字列を書き込む（Exclusive lock 取得、WAL）。
     */
    public void setString(BlockId blk, int offset, String newVal) {
        lockMgr.xLock(blk);  // ← Exclusive lock 取得
        
        Buffer buf = bm.pin(blk);
        try {
            Page p = buf.contents();
            String old = p.getString(offset);

            // TODO: WAL for setString
            // log.append(LogCodec.setString(txId, blk.filename(), blk.number(), offset, old));
            // log.flush(0);

            p.setString(offset, newVal);
            buf.setDirty();
            buf.flushIfDirty();
        } finally {
            bm.unpin(buf);
        }
    }

    /**
     * Commit: ログ記録後、すべてのロック解放。
     */
    public void commit() {
        log.append(LogCodec.commit(txId));
        log.flush(0);
        lockMgr.unlockAll();  // ← ロック解放
    }

    /**
     * Rollback: UNDO 実行後、すべてのロック解放。
     */
    public void rollback() {
        // ... 既存の rollback ロジック ...
        
        lockMgr.unlockAll();  // ← ロック解放
        log.append(LogCodec.rollback(txId));
        log.flush(0);
    }

    @Override
    public void close() {
        lockMgr.unlockAll();  // 念のためロック解放
    }
}
```

---

### 6. IsolationLevel 実装

```java
// src/main/java/app/tx/isolation/IsolationLevel.java
package app.tx.isolation;

/**
 * SQL標準のトランザクション分離レベル。
 */
public enum IsolationLevel {
    /**
     * READ UNCOMMITTED:
     * - Dirty Read 許可
     * - Lost Update 可能
     * - 最低限のロックのみ
     */
    READ_UNCOMMITTED(0),

    /**
     * READ COMMITTED (デフォルト):
     * - Dirty Read 防止
     * - Non-repeatable Read 可能
     * - 読み取り後すぐに Shared lock 解放
     */
    READ_COMMITTED(1),

    /**
     * REPEATABLE READ:
     * - Dirty Read, Non-repeatable Read 防止
     * - Phantom Read 可能
     * - Shared lock を commit まで保持
     */
    REPEATABLE_READ(2),

    /**
     * SERIALIZABLE:
     * - すべての異常を防止
     * - Predicate lock（範囲ロック）使用
     * - 最も厳格
     */
    SERIALIZABLE(3);

    private final int level;

    IsolationLevel(int level) {
        this.level = level;
    }

    public int getLevel() {
        return level;
    }

    public boolean allowsDirtyRead() {
        return this == READ_UNCOMMITTED;
    }

    public boolean allowsNonRepeatableRead() {
        return this == READ_UNCOMMITTED || this == READ_COMMITTED;
    }

    public boolean allowsPhantomRead() {
        return this != SERIALIZABLE;
    }
}
```

---

## 🧪 テストケース

### 基本ロックテスト

```java
// src/test/java/app/tx/lock/LockTest.java
package app.tx.lock;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.*;

class LockTest {

    @Test
    void multipleSharedLocksAllowed() throws Exception {
        Lock lock = new Lock();
        
        assertTrue(lock.acquireShared(1, 1000));
        assertTrue(lock.acquireShared(2, 1000));
        assertTrue(lock.acquireShared(3, 1000));
        
        lock.release(1);
        lock.release(2);
        lock.release(3);
    }

    @Test
    void exclusiveLockBlocksOthers() throws Exception {
        Lock lock = new Lock();
        
        assertTrue(lock.acquireExclusive(1, 1000));
        assertFalse(lock.acquireShared(2, 100)); // タイムアウト
        
        lock.release(1);
        assertTrue(lock.acquireShared(2, 1000)); // 成功
    }

    @Test
    void sharedToExclusiveUpgrade() throws Exception {
        Lock lock = new Lock();
        
        assertTrue(lock.acquireShared(1, 1000));
        assertTrue(lock.acquireExclusive(1, 1000)); // アップグレード
        
        lock.release(1);
    }
}
```

### 並行トランザクションテスト

```java
// src/test/java/app/tx/ConcurrencyTest.java
package app.tx;

import app.memory.BufferMgr;
import app.memory.LogManager;
import app.storage.BlockId;
import app.storage.FileMgr;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class ConcurrencyTest {
    
    @TempDir
    Path tempDir;
    
    private FileMgr fm;
    private BufferMgr bm;
    private LogManager log;

    @BeforeEach
    void setUp() {
        fm = new FileMgr(tempDir, 4096);
        bm = new BufferMgr(fm, 4096, 10);
        log = new LogManager(fm, "test.log");
    }

    @Test
    @Timeout(5)
    void lostUpdatePrevention() throws Exception {
        BlockId blk = new BlockId("test.dat", 0);
        
        // 初期値設定
        try (Tx tx0 = new Tx(fm, bm, log, tempDir)) {
            tx0.setInt(blk, 0, 100);
            tx0.commit();
        }

        // 2つのトランザクションが同時に更新
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);

        Future<Void> f1 = executor.submit(() -> {
            try (Tx tx1 = new Tx(fm, bm, log, tempDir)) {
                int val = tx1.getInt(blk, 0);
                Thread.sleep(100); // 意図的に遅延
                tx1.setInt(blk, 0, val + 10);
                tx1.commit();
                latch.countDown();
            } catch (Exception e) {
                fail(e);
            }
            return null;
        });

        Future<Void> f2 = executor.submit(() -> {
            try (Tx tx2 = new Tx(fm, bm, log, tempDir)) {
                int val = tx2.getInt(blk, 0);
                Thread.sleep(100); // 意図的に遅延
                tx2.setInt(blk, 0, val + 20);
                tx2.commit();
                latch.countDown();
            } catch (Exception e) {
                fail(e);
            }
            return null;
        });

        latch.await();
        executor.shutdown();

        // 最終値を確認: 100 + 10 + 20 = 130
        try (Tx txFinal = new Tx(fm, bm, log, tempDir)) {
            int finalVal = txFinal.getInt(blk, 0);
            assertEquals(130, finalVal, "Lost update occurred!");
        }
    }

    @Test
    @Timeout(5)
    void dirtyReadPrevention() throws Exception {
        BlockId blk = new BlockId("test.dat", 1);
        
        // 初期値
        try (Tx tx0 = new Tx(fm, bm, log, tempDir)) {
            tx0.setInt(blk, 0, 50);
            tx0.commit();
        }

        CountDownLatch tx1Started = new CountDownLatch(1);
        CountDownLatch tx2CanRead = new CountDownLatch(1);

        ExecutorService executor = Executors.newFixedThreadPool(2);

        // Tx1: 更新してロールバック
        Future<Void> f1 = executor.submit(() -> {
            try (Tx tx1 = new Tx(fm, bm, log, tempDir)) {
                tx1.setInt(blk, 0, 999); // Dirty value
                tx1Started.countDown();
                tx2CanRead.await(); // Tx2 が読み取りを試みるのを待つ
                Thread.sleep(200);
                tx1.rollback(); // ロールバック
            } catch (Exception e) {
                fail(e);
            }
            return null;
        });

        // Tx2: Tx1 がロールバックする前に読み取り試行
        Future<Integer> f2 = executor.submit(() -> {
            try {
                tx1Started.await();
                tx2CanRead.countDown();
                try (Tx tx2 = new Tx(fm, bm, log, tempDir)) {
                    // Tx1 の Exclusive lock が解放されるまで待機
                    return tx2.getInt(blk, 0);
                }
            } catch (Exception e) {
                fail(e);
                return -1;
            }
        });

        int readValue = f2.get();
        assertEquals(50, readValue, "Dirty read occurred!");

        executor.shutdown();
    }
}
```

---

## 📊 パフォーマンス考慮事項

### ロック粒度

現在の実装は **ブロックレベルロック**:

- ✅ 実装が単純
- ✅ デッドロック検出が容易
- ❌ 並行性が低い（同じブロック内の異なるレコードも排他）

**将来的な改善**:

- レコードレベルロック
- テーブルレベルロック（フルスキャン時）
- 意図ロック（Intention Lock）

### デッドロック対策

現在の実装:

- タイムアウトベース

**改善案**:

- Wait-For Graph による検出
- デッドロックの自動解消（Victim 選択とロールバック）
- ロック取得順序の統一（テーブル名順、ブロック番号順など）

---

## 🎯 次のステップ

Phase 1 完了後:

1. **Phase 2: Recovery Manager 実装**
   - UNDO/REDO ログ拡張
   - チェックポイント機構
   - クラッシュリカバリ

2. **統合テスト**
   - ロック + リカバリの複合テスト
   - 大規模並行トランザクションテスト

3. **ドキュメント更新**
   - README にロック機能追加
   - API ドキュメント整備

---

**最終更新**: 2025-11-16  
**担当フェーズ**: Phase 1  
**次期目標**: Phase 2 リカバリ機能
