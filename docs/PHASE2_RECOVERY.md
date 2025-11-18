# Phase 2: Recovery Manager Architecture

## 概要

Phase 2では、クラッシュリカバリとチェックポイント機能を実装しました。これにより、システム障害発生時にデータの一貫性を保ち、未コミットのトランザクションを自動的にロールバックできます。

## 実装範囲

### 新規実装コンポーネント

1. **LogType拡張**
   - `SET_STRING`: 文字列更新のUNDOログ
   - `CHECKPOINT`: チェックポイントログ（アクティブTxリスト付き）

2. **RecoveryManager** (`app.tx.recovery.RecoveryManager`)
   - システム起動時のクラッシュリカバリ
   - UNDOログの自動適用
   - チェックポイントからの効率的なリカバリ

3. **CheckpointManager** (`app.tx.recovery.CheckpointManager`)
   - アクティブトランザクションの追跡
   - 定期的なチェックポイント実行
   - バックグラウンドスレッドでの自動実行

### 拡張コンポーネント

- **LogCodec**: SET_STRING, CHECKPOINTログのシリアライズ/デシリアライズ
- **Tx**: SET_STRINGログのrollback処理対応

---

## システムアーキテクチャ

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                         │
│  ┌───────────────────────────────────────────────────────┐  │
│  │  Tx (Transaction)                                     │  │
│  │   - commit() / rollback()                             │  │
│  │   - setInt() / setString() with UNDO logging          │  │
│  └───────────────────────────────────────────────────────┘  │
└────────────────────────┬────────────────────────────────────┘
                         │
        ┌────────────────┼────────────────┐
        ↓                ↓                ↓
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│ BufferMgr    │  │ LogManager   │  │ LockManager  │
│ (Page Cache) │  │ (WAL)        │  │ (2PL)        │
└──────────────┘  └──────────────┘  └──────────────┘
        │                ↓                │
        │         ┌──────────────┐        │
        │         │  LogCodec    │        │
        │         │  (Serialize) │        │
        │         └──────────────┘        │
        ↓                ↓                ↓
┌─────────────────────────────────────────────────────┐
│            Recovery Layer (Phase 2)                 │
│  ┌─────────────────────────────────────────────┐   │
│  │  RecoveryManager                            │   │
│  │   - recover()                               │   │
│  │   - undoSetInt() / undoSetString()          │   │
│  │   - Checkpoint-aware recovery               │   │
│  └─────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────┐   │
│  │  CheckpointManager                          │   │
│  │   - registerTransaction()                   │   │
│  │   - unregisterTransaction()                 │   │
│  │   - checkpoint()                            │   │
│  │   - Periodic background execution           │   │
│  └─────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
        │                                    │
        ↓                                    ↓
┌──────────────┐                    ┌──────────────┐
│  FileMgr     │                    │  Active Tx   │
│  (Disk I/O)  │                    │  Tracking    │
└──────────────┘                    └──────────────┘
```

---

## ログレコード構造

### 既存ログタイプ（Phase 1）

1. **START**: `[type:int][txId:int]`
2. **COMMIT**: `[type:int][txId:int]`
3. **ROLLBACK**: `[type:int][txId:int]`
4. **SET_INT**: `[type:int][txId:int][fnameLen:int][fname:bytes][blk:int][offset:int][oldVal:int]`

### 新規ログタイプ（Phase 2）

5. **SET_STRING**: `[type:int][txId:int][fnameLen:int][fname:bytes][blk:int][offset:int][oldValLen:int][oldVal:bytes]`
6. **CHECKPOINT**: `[type:int][0:int][txCount:int][tx1:int][tx2:int]...[txN:int]`

---

## リカバリアルゴリズム

### 1. クラッシュリカバリフロー

```
System Start
     │
     ↓
┌─────────────────────────────────────────┐
│  RecoveryManager.recover()              │
└─────────────────────────────────────────┘
     │
     ↓
┌─────────────────────────────────────────┐
│  Phase 1: Identify Active Transactions  │
│  ┌─────────────────────────────────┐   │
│  │  Scan log from newest to oldest │   │
│  │                                  │   │
│  │  For each log record:            │   │
│  │   - COMMIT/ROLLBACK              │   │
│  │     → Add txId to committed set  │   │
│  │   - CHECKPOINT                   │   │
│  │     → Use active tx list         │   │
│  │   - START                        │   │
│  │     → If not committed, mark     │   │
│  │       as active (needs UNDO)     │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
     │
     ↓
┌─────────────────────────────────────────┐
│  Phase 2: UNDO Active Transactions      │
│  ┌─────────────────────────────────┐   │
│  │  For each active txId:           │   │
│  │                                  │   │
│  │  Scan log backward:              │   │
│  │   - SET_INT                      │   │
│  │     → Restore oldValInt          │   │
│  │   - SET_STRING                   │   │
│  │     → Restore oldValString       │   │
│  │   - START                        │   │
│  │     → UNDO complete for this tx  │   │
│  └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
     │
     ↓
Recovery Complete
```

### 2. チェックポイントフロー

```
Checkpoint Trigger (periodic or manual)
     │
     ↓
┌──────────────────────────────────────────┐
│  CheckpointManager.checkpoint()          │
└──────────────────────────────────────────┘
     │
     ↓
┌──────────────────────────────────────────┐
│  Step 1: Get Active Tx Snapshot          │
│  ┌────────────────────────────────────┐  │
│  │  activeTxIds = current active set  │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
     │
     ↓
┌──────────────────────────────────────────┐
│  Step 2: Write CHECKPOINT Log            │
│  ┌────────────────────────────────────┐  │
│  │  LogCodec.checkpoint(activeTxIds)  │  │
│  │  → [CHECKPOINT][0][count][tx1]...  │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
     │
     ↓
┌──────────────────────────────────────────┐
│  Step 3: Flush Log to Disk               │
│  ┌────────────────────────────────────┐  │
│  │  LogManager.flush(lsn)             │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
     │
     ↓
Checkpoint Complete
```

### 3. リカバリ時のチェックポイント利用

```
Recovery with Checkpoint
     │
     ↓
┌──────────────────────────────────────────┐
│  Scan log backward                       │
│  ┌────────────────────────────────────┐  │
│  │  activeTxIds = {}                  │  │
│  │  checkpointActiveTxIds = null      │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
     │
     ↓
┌──────────────────────────────────────────┐
│  Find CHECKPOINT?                        │
└──────────────────────────────────────────┘
     │ No                │ Yes
     ↓                   ↓
Continue         ┌──────────────────────────┐
scanning         │  checkpointActiveTxIds = │
until START      │  checkpoint.activeTxIds  │
                 └──────────────────────────┘
     │                   │
     └───────┬───────────┘
             ↓
┌──────────────────────────────────────────┐
│  Compute txsToUndo                       │
│  ┌────────────────────────────────────┐  │
│  │  if (checkpointActiveTxIds != null)│  │
│  │    txsToUndo = activeTxIds         │  │
│  │                ∩                    │  │
│  │                checkpointActiveTxIds│  │
│  │  else                              │  │
│  │    txsToUndo = activeTxIds         │  │
│  └────────────────────────────────────┘  │
└──────────────────────────────────────────┘
     │
     ↓
UNDO only txsToUndo (reduced set)
```

---

## トランザクションライフサイクルとチェックポイント

```
Timeline:  T0 ────────→ T1 ───────→ T2 ────────→ T3 ───→ CRASH ───→ RECOVERY

Tx1:       START───────────────────────COMMIT
           │                           │
           └─────────── (Complete) ─────┘

Tx2:       ────START───────────────────────────────────ROLLBACK
                │                                       │
                └──────────── (Complete) ───────────────┘

Tx3:       ─────────START──────────────────────────────────────── (Active)
                    │                                             ↓
                    └───────────────────────────────────────→ UNDO

Checkpoint: ───────────────────CHECKPOINT([Tx2, Tx3])
                               │
                               └─→ Record active: {Tx2, Tx3}

Recovery:  ─────────────────────────────────────────────────────→
           Scan backward:
           - Tx1: COMMIT found → No UNDO
           - Tx2: ROLLBACK found → No UNDO
           - Tx3: No COMMIT/ROLLBACK → UNDO
           - Use CHECKPOINT to limit scan
```

---

## 性能特性

### リカバリ時間

- **Without Checkpoint**: O(log_size) - 全ログをスキャン
- **With Checkpoint**: O(log_since_checkpoint) - チェックポイント以降のみ

### チェックポイントコスト

- **CPU**: O(active_tx_count) - アクティブTxリストのスナップショット取得
- **I/O**: O(1) - 1回のログ書き込み + フラッシュ

### メモリ使用量

- **RecoveryManager**: O(log_records + active_tx_count) - ログスキャン時の一時バッファ
- **CheckpointManager**: O(active_tx_count) - アクティブTxリスト

---

## エラーハンドリング

### 1. リカバリ中のエラー

```java
try {
    recoveryManager.recover();
} catch (Exception e) {
    // Fatal: システム起動不可
    System.err.println("Recovery failed: " + e.getMessage());
    System.exit(1);
}
```

### 2. チェックポイント失敗

```java
try {
    checkpointManager.checkpoint();
} catch (Exception e) {
    // Non-fatal: 次回のチェックポイントで再試行
    System.err.println("Checkpoint failed: " + e.getMessage());
    // Continue normal operation
}
```

---

## 実装の制約と将来の拡張

### 現在の実装（Phase 2）

- ✅ **UNDO-Only Recovery**: 未コミットトランザクションのロールバック
- ✅ **Checkpoint**: アクティブTxリストの記録
- ✅ **SET_INT/SET_STRING**: 整数と文字列のUNDO
- ❌ **REDO Log**: 未実装（コミット済みトランザクションの再適用）
- ❌ **Physical REDO**: ページレベルのREDO未対応
- ❌ **Logical Logging**: 論理操作のログ未対応

### 将来の拡張（Phase 2完全版）

1. **REDO処理**
   - コミット済みだがディスクに未反映のトランザクションを再適用
   - UNDO/REDO両方のリカバリ

2. **Fuzzy Checkpoint**
   - チェックポイント実行中もトランザクション継続可能
   - より低いオーバーヘッド

3. **Archive Log**
   - 古いログのアーカイブ
   - ポイント・イン・タイム・リカバリ

---

## テスト戦略

### 1. 単体テスト (`RecoveryManagerTest`)

- ✅ **testRecoveryManager_BasicOperation**: 基本動作確認
- ✅ **testCheckpointManager_BasicOperation**: チェックポイント機能
- ✅ **testLogCodec_NewLogTypes**: 新しいログタイプのシリアライズ

### 2. デモプログラム (`RecoveryDemo`)

- ✅ **Scenario 1**: 未コミットトランザクションのUNDO
- ✅ **Scenario 2**: コミット済みトランザクションの維持
- ✅ **Scenario 3**: チェックポイントからのリカバリ

### 3. 統合テスト（将来）

- 複数トランザクションの同時実行とクラッシュ
- 大量ログでのリカバリ性能測定
- チェックポイント間隔の最適化

---

## 使用方法

### RecoveryManagerの使用

```java
// システム起動時に1回実行
FileMgr fm = new FileMgr(dataDir, 400);
LogManager lm = new LogManager(logDir);
BufferMgr bm = new BufferMgr(fm, 400, 8);

RecoveryManager recoveryMgr = new RecoveryManager(logDir.toFile(), bm);
recoveryMgr.recover(); // 自動的に未コミットTxをUNDO
```

### CheckpointManagerの使用

```java
// システム起動時に開始
CheckpointManager checkpointMgr = new CheckpointManager(lm, 30); // 30秒間隔
checkpointMgr.start();

// トランザクション開始時
checkpointMgr.registerTransaction(tx.id());

// トランザクション終了時
checkpointMgr.unregisterTransaction(tx.id());

// システム終了時
checkpointMgr.stop();
```

### デモ実行

```bash
# コンパイル
./gradlew compileJava

# デモ実行
java -cp "build/classes/java/main" app.example.RecoveryDemo
```

---

## まとめ

Phase 2では、以下を実装しました:

1. **RecoveryManager**: クラッシュリカバリの自動実行
2. **CheckpointManager**: 効率的なリカバリのためのチェックポイント
3. **LogCodec拡張**: SET_STRING, CHECKPOINTログのサポート
4. **Tx統合**: SET_STRINGログのrollback対応

これにより、MiniDBは以下の特性を持つようになりました:

- ✅ **Durability**: システムクラッシュ後もデータの一貫性を保証
- ✅ **Atomicity**: 未コミットトランザクションの自動ロールバック
- ✅ **Efficiency**: チェックポイントによる高速リカバリ

次のステップ（Phase 3）では、JDBC インターフェースの実装により、標準的なJavaアプリケーションからMiniDBにアクセスできるようになります。

---

**Branch**: `feature/phase2-recovery-manager`  
**Version**: v0.21.0  
**Date**: 2025-11-18  
**Status**: ✅ Complete
