# Phase 1: 並行制御の完全実装 - アーキテクチャドキュメント

## 概要

Phase 1では、マルチスレッド環境でのデータ整合性を確保する並行制御機能を完全に実装しました。本ドキュメントでは、Phase 1全体のアーキテクチャ、データフロー、および各コンポーネントの関係を視覚的に説明します。

---

## 実装スコープ

### Week 1: 基本的なロック機能
- **Lock**: 共有ロック（S-Lock）と排他ロック（X-Lock）
- **LockTable**: ブロック単位のロック管理
- **LockManager**: トランザクション単位のロック管理
- **Strict 2PL**: Strict Two-Phase Locking プロトコル

### Week 2: デッドロック検出と分離レベル
- **WaitForGraph**: Wait-For グラフによるデッドロック検出
- **DeadlockDetector**: 周期的なデッドロック検出と自動解決
- **IsolationLevel**: 4つの分離レベル（READ_UNCOMMITTED, READ_COMMITTED, REPEATABLE_READ, SERIALIZABLE）

### Week 3: TableScan統合
- **TableScan**: レコードレベルでの自動ロック管理
- トランザクション統合による透過的なロック制御

---

## システムアーキテクチャ

```
┌──────────────────────────────────────────────────────────────────────────┐
│                           Application Layer                              │
│                         (SQL, Query Planner)                             │
└─────────────────────────────────┬────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                         Record Management Layer                          │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │  TableScan                                                       │   │
│  │  ┌──────────┬──────────┬──────────┬──────────┬──────────┐       │   │
│  │  │ next()   │ getInt() │getString()│ setInt() │ insert() │       │   │
│  │  └────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬─────┘       │   │
│  │       │          │          │          │          │               │   │
│  │       │ S-Lock   │ S-Lock   │ S-Lock   │ X-Lock   │ X-Lock        │   │
│  │       │          │          │(Level依存)│          │               │   │
│  └───────┼──────────┼──────────┼──────────┼──────────┼───────────────┘   │
└──────────┼──────────┼──────────┼──────────┼──────────┼───────────────────┘
           │          │          │          │          │
           └──────────┴──────────┴──────────┴──────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                        Transaction Layer (Phase 1)                       │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  Tx (Transaction)                                                  │  │
│  │  ┌────────┬─────────┬──────────┬─────────────┬───────────────┐   │  │
│  │  │ begin  │ commit  │ rollback │ getInt()    │ setInt()      │   │  │
│  │  │        │         │          │ getString() │ setString()   │   │  │
│  │  └────────┴─────────┴──────────┴─────────────┴───────────────┘   │  │
│  │                                  ▲                                 │  │
│  │                                  │                                 │  │
│  │     ┌────────────────────────────┴─────────────────────────────┐  │  │
│  │     │                                                            │  │  │
│  │     │  IsolationLevel                 LockManager               │  │  │
│  │     │  ┌──────────────────┐          ┌──────────────────────┐  │  │  │
│  │     │  │ READ_UNCOMMITTED │          │ sLock(BlockId)       │  │  │  │
│  │     │  │ READ_COMMITTED   │          │ xLock(BlockId)       │  │  │  │
│  │     │  │ REPEATABLE_READ  │          │ unlock(BlockId)      │  │  │  │
│  │     │  │ SERIALIZABLE     │          │ release()            │  │  │  │
│  │     │  └──────────────────┘          └───────────┬──────────┘  │  │  │
│  │     │                                              │             │  │  │
│  │     └──────────────────────────────────────────────┼─────────────┘  │  │
│  │                                                     │                │  │
│  │                                                     ▼                │  │
│  │                                         ┌──────────────────────┐    │  │
│  │                                         │  LockTable           │    │  │
│  │                                         │  (Block-level locks) │    │  │
│  │                                         │  ┌────────────────┐  │    │  │
│  │                                         │  │ BlockId → Lock │  │    │  │
│  │                                         │  └────────────────┘  │    │  │
│  │                                         └──────────┬───────────┘    │  │
│  │                                                    │                │  │
│  │                                                    ▼                │  │
│  │                                         ┌──────────────────────┐    │  │
│  │                                         │  Lock                │    │  │
│  │                                         │  ┌────────────────┐  │    │  │
│  │                                         │  │ S-Lock (共有)  │  │    │  │
│  │                                         │  │ X-Lock (排他)  │  │    │  │
│  │                                         │  └────────────────┘  │    │  │
│  │                                         └──────────────────────┘    │  │
│  └────────────────────────────────────────────────────────────────────┘  │
│                                                                           │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │  DeadlockDetector (Background Thread)                             │  │
│  │  ┌──────────┐     ┌──────────────────┐     ┌─────────────────┐   │  │
│  │  │ start()  │────▶│ detectAndResolve │────▶│ selectVictim()  │   │  │
│  │  └──────────┘     └──────────────────┘     └─────────────────┘   │  │
│  │                             │                                      │  │
│  │                             ▼                                      │  │
│  │                    ┌──────────────────┐                            │  │
│  │                    │  WaitForGraph    │                            │  │
│  │                    │  ┌────────────┐  │                            │  │
│  │                    │  │addEdge()   │  │                            │  │
│  │                    │  │detectCycle()│  │                            │  │
│  │                    │  │removeNode()│  │                            │  │
│  │                    │  └────────────┘  │                            │  │
│  │                    └──────────────────┘                            │  │
│  └────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
┌──────────────────────────────────────────────────────────────────────────┐
│                          Storage Layer                                   │
│  ┌────────────────┬──────────────────┬────────────────────────────┐     │
│  │  FileMgr       │  BufferMgr       │  LogManager               │     │
│  │  (Block I/O)   │  (Buffer Pool)   │  (Write-Ahead Logging)    │     │
│  └────────────────┴──────────────────┴────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────┘
```

---

## ロック獲得フロー

### S-Lock (共有ロック) 獲得フロー

```
┌─────────────────────────────────────────────────────────────────┐
│ Client Code                                                     │
│   tx.getInt(blockId, offset)                                    │
│   または                                                        │
│   tableScan.getInt("fieldName")                                 │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Tx Layer                                                         │
│   1. Check isolation level                                       │
│      - READ_UNCOMMITTED: ロックなし → 直接読み取り              │
│      - READ_COMMITTED: S-Lock取得 → 読取後即座に解放            │
│      - REPEATABLE_READ: S-Lock取得 → コミットまで保持           │
│      - SERIALIZABLE: S-Lock取得 → コミットまで保持              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ LockManager                                                      │
│   lockMgr.sLock(blockId)                                         │
│   1. Check if already holding lock                               │
│   2. Request S-Lock from LockTable                               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ LockTable                                                        │
│   lockTable.sLock(blockId, txId)                                 │
│   1. Get or create Lock for blockId                              │
│   2. Acquire S-Lock (wait if X-Lock held)                        │
│   3. Update waitForGraph (if waiting)                            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Lock                                                             │
│   lock.sLock(txId)                                               │
│   1. Check if S-Lock can be granted                              │
│      - S-Lock holders: Yes (compatible)                          │
│      - X-Lock holder: No (wait)                                  │
│   2. Add txId to sLockHolders                                    │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Success: S-Lock acquired                                         │
│   - Read data from BufferMgr                                     │
│   - Release lock (if READ_COMMITTED)                             │
│   - Hold lock until commit (if REPEATABLE_READ/SERIALIZABLE)    │
└──────────────────────────────────────────────────────────────────┘
```

### X-Lock (排他ロック) 獲得フロー

```
┌─────────────────────────────────────────────────────────────────┐
│ Client Code                                                     │
│   tx.setInt(blockId, offset, value)                             │
│   または                                                        │
│   tableScan.setInt("fieldName", value)                          │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Tx Layer                                                         │
│   1. All write operations require X-Lock                         │
│   2. No isolation level check (always acquire)                   │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ LockManager                                                      │
│   lockMgr.xLock(blockId)                                         │
│   1. Check if already holding lock                               │
│      - S-Lock: Upgrade to X-Lock                                 │
│      - X-Lock: Already held, continue                            │
│   2. Request X-Lock from LockTable                               │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ LockTable                                                        │
│   lockTable.xLock(blockId, txId)                                 │
│   1. Get or create Lock for blockId                              │
│   2. Acquire X-Lock (wait if any locks held by others)           │
│   3. Update waitForGraph (if waiting)                            │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Lock                                                             │
│   lock.xLock(txId)                                               │
│   1. Check if X-Lock can be granted                              │
│      - No holders: Yes                                           │
│      - Only self holding S-Lock: Upgrade                         │
│      - Others holding locks: No (wait)                           │
│   2. Set xLockHolder = txId                                      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────┐
│ Success: X-Lock acquired                                         │
│   - Write data to BufferMgr (with WAL)                           │
│   - Hold lock until commit/rollback                              │
└──────────────────────────────────────────────────────────────────┘
```

---

## デッドロック検出フロー

```
┌──────────────────────────────────────────────────────────────────────────┐
│ DeadlockDetector (Background Thread - 100ms interval)                    │
└──────────────────────────────┬───────────────────────────────────────────┘
                               │
                               ▼
                    ┌──────────────────────────┐
                    │ detectAndResolve()       │
                    └──────────┬───────────────┘
                               │
                               ▼
         ┌─────────────────────┴─────────────────────┐
         │                                           │
         ▼                                           ▼
┌──────────────────────┐                  ┌──────────────────────┐
│ 1. Build WaitForGraph│                  │ 2. Detect Cycle      │
│                      │                  │                      │
│ For each Tx:         │                  │ Use DFS algorithm    │
│   For each BlockId:  │                  │ to find cycles       │
│     Get waiting Tx   │                  │                      │
│     Add edge:        │                  │                      │
│     waiter → holder  │                  │                      │
└──────────┬───────────┘                  └──────────┬───────────┘
           │                                         │
           └─────────────────┬───────────────────────┘
                             │
                             ▼
                ┌────────────────────────┐
                │ Cycle detected?        │
                └────────┬───────────────┘
                         │
                  Yes ───┼─── No → Continue monitoring
                         │
                         ▼
                ┌────────────────────────┐
                │ 3. Select Victim       │
                │                        │
                │ Strategy:              │
                │ Choose Tx with         │
                │ maximum ID in cycle    │
                └────────┬───────────────┘
                         │
                         ▼
                ┌────────────────────────┐
                │ 4. Abort Victim        │
                │                        │
                │ - Mark tx as aborted   │
                │ - Release all locks    │
                │ - Log event            │
                └────────┬───────────────┘
                         │
                         ▼
                ┌────────────────────────┐
                │ 5. Other Tx proceeds   │
                │                        │
                │ Waiting Tx acquires    │
                │ lock and continues     │
                └────────────────────────┘
```

---

## 分離レベル別ロック動作

### READ_UNCOMMITTED

```
Transaction:
  begin
  ├─ read (No lock required)
  ├─ read (No lock required)
  └─ commit
```

**特性**:
- Dirty Read: 可能
- Non-Repeatable Read: 可能
- Phantom Read: 可能

### READ_COMMITTED

```
Transaction:
  begin
  ├─ read
  │   ├─ Acquire S-Lock
  │   ├─ Read data
  │   └─ Release S-Lock immediately
  ├─ read
  │   ├─ Acquire S-Lock
  │   ├─ Read data
  │   └─ Release S-Lock immediately
  └─ commit
```

**特性**:
- Dirty Read: 不可（S-Lock取得）
- Non-Repeatable Read: 可能（ロック即座に解放）
- Phantom Read: 可能

### REPEATABLE_READ

```
Transaction:
  begin
  ├─ read
  │   ├─ Acquire S-Lock
  │   └─ Read data (lock held)
  ├─ read
  │   ├─ Acquire S-Lock
  │   └─ Read data (lock held)
  ├─ write
  │   ├─ Upgrade S-Lock → X-Lock
  │   └─ Write data (lock held)
  └─ commit
      └─ Release all locks
```

**特性**:
- Dirty Read: 不可
- Non-Repeatable Read: 不可（コミットまでロック保持）
- Phantom Read: 可能

### SERIALIZABLE

```
Transaction:
  begin
  ├─ read range (predicate lock ready)
  │   ├─ Acquire S-Lock on all matching blocks
  │   └─ Read data (locks held)
  ├─ write
  │   ├─ Acquire X-Lock
  │   └─ Write data (lock held)
  └─ commit
      └─ Release all locks
```

**特性**:
- Dirty Read: 不可
- Non-Repeatable Read: 不可
- Phantom Read: 不可（述語ロック対応準備完了）

---

## トランザクションライフサイクル

```
┌────────────────────────────────────────────────────────────────┐
│                    Transaction Lifecycle                       │
└────────────────────────────────────────────────────────────────┘

    START
      │
      ▼
┌──────────────────┐
│ Tx created       │
│ - Assign ID      │
│ - Set Isolation  │
│ - Initialize     │
│   LockManager    │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Growing Phase    │  ◀────────────────────┐
│ - Acquire locks  │                       │
│ - Read/Write     │                       │
│   operations     │                       │
└────────┬─────────┘                       │
         │                                 │
         ├─ More operations? ──────────────┘
         │
         ▼
    ┌─────────┐
    │ Commit? │
    └────┬────┘
         │
    ┌────┴────┐
    │         │
   Yes       No
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│ Commit │ │ Rollback │
└───┬────┘ └────┬─────┘
    │           │
    │           ▼
    │      ┌──────────────────┐
    │      │ Undo Changes     │
    │      │ - Replay undo log│
    │      │ - Restore values │
    │      └────────┬─────────┘
    │               │
    └───────┬───────┘
            │
            ▼
    ┌──────────────────┐
    │ Shrinking Phase  │
    │ - Release all    │
    │   locks          │
    │ - Remove from    │
    │   WaitForGraph   │
    └────────┬─────────┘
             │
             ▼
          END
```

---

## ロック互換性マトリクス

```
┌─────────────────────────────────────────┐
│        Lock Compatibility Matrix        │
└─────────────────────────────────────────┘

          │ No Lock │ S-Lock │ X-Lock │
──────────┼─────────┼────────┼────────┤
 No Lock  │    ✅   │   ✅   │   ✅   │
──────────┼─────────┼────────┼────────┤
 S-Lock   │    ✅   │   ✅   │   ❌   │
──────────┼─────────┼────────┼────────┤
 X-Lock   │    ✅   │   ❌   │   ❌   │
──────────┴─────────┴────────┴────────┘

✅ = Compatible (can proceed)
❌ = Incompatible (must wait)
```

---

## パフォーマンス特性

### ロック操作の計算量

| 操作 | 計算量 | 説明 |
|------|--------|------|
| S-Lock獲得 | O(1) | HashMap lookup + synchronized block |
| X-Lock獲得 | O(1) | HashMap lookup + synchronized block |
| ロック解放 | O(n) | n = トランザクションが保持するロック数 |
| デッドロック検出 | O(V + E) | V = トランザクション数, E = エッジ数（DFS） |

### メモリ使用量

| コンポーネント | メモリ | 説明 |
|----------------|--------|------|
| Lock | 40-80 bytes | txId Set + fields |
| LockTable | HashMap + Locks | O(unique blocks) |
| LockManager | HashMap | O(transactions) |
| WaitForGraph | HashMap + HashSet | O(transactions + waiting edges) |

---

## エラーハンドリング

### タイムアウト

```
Tx tries to acquire lock
    │
    ▼
┌──────────────────┐
│ Wait for lock    │  ← Max 10 seconds
└────────┬─────────┘
         │
    Timeout?
         │
    ┌────┴────┐
    │         │
   Yes       No
    │         │
    ▼         ▼
┌────────┐ ┌──────────┐
│ Throw  │ │ Continue │
│ Lock   │ │          │
│ Abort  │ │          │
│ Exception│ │          │
└────────┘ └──────────┘
```

### デッドロック

```
DeadlockDetector detects cycle
    │
    ▼
┌──────────────────┐
│ Select victim    │
│ (max ID in cycle)│
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Mark as aborted  │
│ in WaitForGraph  │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Victim Tx throws │
│ LockAbort        │
│ Exception        │
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Application      │
│ catches and      │
│ handles rollback │
└──────────────────┘
```

---

## テスト戦略

### Unit Tests (59 tests)

1. **Lock クラス** (10 tests)
   - S-Lock/X-Lock 取得
   - ロック互換性
   - ロックアップグレード
   - タイムアウト

2. **LockTable クラス** (6 tests)
   - ブロック単位のロック管理
   - 複数トランザクションの並行制御
   - ロック解放

3. **LockManager クラス** (9 tests)
   - トランザクション単位のロック管理
   - 複数ブロックのロック
   - ロック情報の取得

4. **並行トランザクション** (5 tests)
   - Lost Update 防止
   - Dirty Read 防止
   - ロックアップグレード
   - 並行アクセス

5. **WaitForGraph** (11 tests)
   - グラフ構築
   - サイクル検出
   - エッジ追加/削除

6. **DeadlockDetector** (7 tests)
   - デッドロック検出
   - Victim 選択
   - 自動解決

7. **IsolationLevel** (11 tests)
   - 分離レベル別動作
   - ロック保持期間
   - SQL名変換

---

## 実装済み機能まとめ

| 機能 | 状態 | Week | 説明 |
|------|------|------|------|
| Lock (S/X) | ✅ | 1 | 共有ロックと排他ロック |
| LockTable | ✅ | 1 | ブロック単位のロック管理 |
| LockManager | ✅ | 1 | トランザクション単位の管理 |
| Strict 2PL | ✅ | 1 | 2相ロックプロトコル |
| WaitForGraph | ✅ | 2 | デッドロック検出用グラフ |
| DeadlockDetector | ✅ | 2 | 自動デッドロック解決 |
| IsolationLevel | ✅ | 2 | 4つの分離レベル |
| TableScan統合 | ✅ | 3 | レコードレベル自動ロック |

---

## 次のステップ（Phase 2）

Phase 2では、クラッシュリカバリ機能を実装します：

1. **RecoveryManager**: UNDO/REDOログ管理
2. **CheckpointManager**: チェックポイント機能
3. **LogRecord拡張**: 新しいログタイプ追加
4. **クラッシュリカバリ**: システム起動時の自動復旧

---

## 参照ドキュメント

- `docs/LOCKING_LOGIC_DIAGRAMS.md`: Week 1詳細ロジック図
- `docs/PHASE1_WEEK2_DEADLOCK_AND_ISOLATION.md`: Week 2詳細説明
- `PROGRESS.md`: 実装進捗管理
- `IMPLEMENTATION_PLAN.md`: 全体実装計画
- `README.md`: リリースノート

---

**作成日**: 2025-11-18  
**バージョン**: v0.19.0  
**Phase 1 完了率**: 80% (24/30タスク)
