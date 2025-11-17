# Phase 1 Week 2: デッドロック検出と分離レベル - ロジック図

## 📚 目次

1. [Wait-For Graph アーキテクチャ](#wait-for-graph-アーキテクチャ)
2. [デッドロック検出フロー](#デッドロック検出フロー)
3. [分離レベルの動作](#分離レベルの動作)
4. [Non-Repeatable Read の防止](#non-repeatable-read-の防止)
5. [システム統合図](#システム統合図)

---

## Wait-For Graph アーキテクチャ

### 概要

Wait-For Graph（待機グラフ）は、トランザクション間の待機関係を有向グラフとして表現します：

- **ノード**: トランザクションID
- **エッジ**: Tx A → Tx B = "Tx AがTx Bの保持するロックを待っている"
- **サイクル**: デッドロックを示す

### データ構造

```
WaitForGraph
├── graph: ConcurrentHashMap<Integer, Set<Integer>>
│   └── Key: 待機側トランザクションID (waiter)
│   └── Value: 保持側トランザクションIDのセット (holders)
│
├── addEdge(waiter, holder)
│   └── waiter → holder のエッジを追加
│
├── removeEdge(waiter, holder)
│   └── 特定のエッジを削除
│
├── removeTransaction(txNum)
│   └── トランザクションに関連するすべてのエッジを削除
│
└── detectCycle()
    └── DFS（深さ優先探索）でサイクルを検出
```

### Wait-For Graph の例

```
通常の状態（サイクルなし）:
  Tx1 → Tx2 → Tx3
  (Tx1がTx2を待機、Tx2がTx3を待機)

デッドロック（サイクルあり）:
  Tx1 → Tx2
   ↑     ↓
   └─────┘
  (Tx1がTx2を待機、Tx2がTx1を待機)

3つのトランザクションのデッドロック:
  Tx1 → Tx2
   ↑     ↓
  Tx3 ←─┘
  (Tx1 → Tx2 → Tx3 → Tx1 のサイクル)
```

---

## デッドロック検出フロー

### 1. サイクル検出アルゴリズム（DFS）

```
detectCycle():
  visited = ∅            // 訪問済みノード
  recStack = ∅           // 再帰スタック（現在のパス）
  path = []              // 現在のパス
  
  for each txNum in graph.keys():
    if txNum ∉ visited:
      cycle = detectCycleDFS(txNum, visited, recStack, path)
      if cycle ≠ null:
        return cycle
  
  return []  // サイクルなし

detectCycleDFS(current, visited, recStack, path):
  visited.add(current)
  recStack.add(current)
  path.append(current)
  
  for each neighbor in graph[current]:
    if neighbor ∉ visited:
      // 再帰的に探索
      cycle = detectCycleDFS(neighbor, visited, recStack, path)
      if cycle ≠ null:
        return cycle
    
    else if neighbor ∈ recStack:
      // サイクル検出！
      cycle = extract_cycle(path, neighbor)
      return cycle
  
  path.remove_last()
  recStack.remove(current)
  return null
```

### 2. デッドロック検出と解決のシーケンス

```
┌──────────────┐     ┌──────────────────┐     ┌───────────────┐
│ DeadlockDetector │ │  WaitForGraph    │     │ LockManager   │
└────────┬─────────┘ └─────────┬────────┘     └───────┬───────┘
         │                      │                      │
         │ (定期的に実行: 500ms間隔)                 │
         │                      │                      │
    [1]  │  detectCycle()       │                      │
         ├─────────────────────>│                      │
         │                      │                      │
         │  <サイクル検出>       │                      │
         │<─────────────────────┤                      │
         │  [Tx2, Tx5, Tx2]     │                      │
         │                      │                      │
    [2]  │  selectVictim()      │                      │
         │  → Tx5 (最大ID)      │                      │
         │                      │                      │
    [3]  │  abortCallback(5)    │                      │
         ├──────────────────────┼─────────────────────>│
         │                      │    rollback(Tx5)     │
         │                      │                      │
    [4]  │                      │<─────────────────────┤
         │                      │  removeTransaction(5)│
         │                      │                      │
         │  <デッドロック解決>    │                      │
```

### 3. Victim 選択戦略

現在の実装:
```
selectVictim(cycle):
  return max(cycle)  // 最大ID = 最も新しいトランザクション
```

将来の拡張可能性:
- **ロック数**: 最も少ないロックを持つトランザクション
- **トランザクション年齢**: 最も新しいトランザクション
- **実行した作業量**: 最も少ない作業をしたトランザクション
- **優先度**: 低優先度のトランザクション

---

## 分離レベルの動作

### 4つの分離レベル

```
┌───────────────────┬──────────────┬───────────────┬──────────────┬──────────────┐
│ 分離レベル         │ Dirty Read   │ Non-Repeatable│ Phantom Read │ ロック動作    │
│                   │              │ Read          │              │              │
├───────────────────┼──────────────┼───────────────┼──────────────┼──────────────┤
│ READ_UNCOMMITTED  │ ❌ 発生可能   │ ❌ 発生可能    │ ❌ 発生可能   │ 読み取りロック│
│                   │              │               │              │ なし          │
├───────────────────┼──────────────┼───────────────┼──────────────┼──────────────┤
│ READ_COMMITTED    │ ✅ 防止       │ ❌ 発生可能    │ ❌ 発生可能   │ 短期読み取り  │
│ (デフォルト)       │              │               │              │ ロック        │
├───────────────────┼──────────────┼───────────────┼──────────────┼──────────────┤
│ REPEATABLE_READ   │ ✅ 防止       │ ✅ 防止        │ ❌ 発生可能   │ 長期読み取り  │
│                   │              │               │              │ ロック        │
├───────────────────┼──────────────┼───────────────┼──────────────┼──────────────┤
│ SERIALIZABLE      │ ✅ 防止       │ ✅ 防止        │ ✅ 防止       │ 範囲ロック    │
│                   │              │               │              │              │
└───────────────────┴──────────────┴───────────────┴──────────────┴──────────────┘
```

### ロックの保持期間

```
READ_UNCOMMITTED:
  getInt(blk, offset):
    // ロックを取得しない
    return buffer.getInt(offset)

READ_COMMITTED:
  getInt(blk, offset):
    lockMgr.sLock(blk, txId)        // 共有ロックを取得
    value = buffer.getInt(offset)
    lockMgr.unlock(blk, txId)        // すぐに解放
    return value

REPEATABLE_READ / SERIALIZABLE:
  getInt(blk, offset):
    lockMgr.sLock(blk, txId)        // 共有ロックを取得
    value = buffer.getInt(offset)
    // ロックを保持（commit/rollbackまで）
    return value
  
  commit():
    lockMgr.release(txId)            // すべてのロック解放
```

---

## Non-Repeatable Read の防止

### シナリオ: READ_COMMITTED での Non-Repeatable Read

```
Time   Tx1 (READ_COMMITTED)           Tx2
─────┼──────────────────────────────┼─────────────────────
T1   │ BEGIN                         │
T2   │ sLock(blk) → READ value=100  │
T3   │ unlock(blk) [即座に解放]      │
T4   │                               │ BEGIN
T5   │                               │ xLock(blk) ✅
T6   │                               │ WRITE value=200
T7   │                               │ COMMIT
T8   │ sLock(blk) → READ value=200  │
T9   │ unlock(blk)                   │
T10  │ COMMIT                        │
     │                               │
結果: 100 ≠ 200 → Non-Repeatable Read 発生！
```

### シナリオ: REPEATABLE_READ での防止

```
Time   Tx1 (REPEATABLE_READ)          Tx2
─────┼──────────────────────────────┼─────────────────────
T1   │ BEGIN                         │
T2   │ sLock(blk) → READ value=100  │
T3   │ [ロックを保持]                 │
T4   │                               │ BEGIN
T5   │                               │ xLock(blk) → ❌ WAIT
T6   │                               │ [待機中...]
T7   │ sLock(blk) → READ value=100  │
T8   │ [ロックを保持]                 │
T9   │ COMMIT → unlock(blk)          │
T10  │                               │ xLock(blk) ✅ 取得
T11  │                               │ WRITE value=200
T12  │                               │ COMMIT
     │                               │
結果: 100 == 100 → Non-Repeatable Read 防止！
```

### デモ出力の解釈

```
--- READ_COMMITTED: Non-Repeatable Read 発生 ---
  📖 Tx7: 1回目の読み取り
     値 = 100
  ✏️  Tx8: 値を変更 (100 → 200)
  📖 Tx7: 2回目の読み取り
     値 = 200
  ❌ Non-Repeatable Read 発生: 100 != 200

→ Tx7は読み取り後すぐにロックを解放したため、Tx8が値を変更できた

--- REPEATABLE_READ: Non-Repeatable Read 防止 ---
  📖 Tx10: 1回目の読み取り
     値 = 100
  ✏️  Tx11: 値を変更 (100 → 200)
  ✏️  エラー: タイムアウト

→ Tx10がコミットまでロックを保持しているため、Tx11は待機
→ タイムアウトまで待機（これが正しい動作）
```

---

## システム統合図

### 全体アーキテクチャ

```
┌─────────────────────────────────────────────────────────────┐
│                         Application                         │
└──────────────────────────────┬──────────────────────────────┘
                               │
                    ┌──────────┴──────────┐
                    │         Tx          │
                    │ (IsolationLevel)    │
                    └──────────┬──────────┘
                               │
        ┌──────────────────────┼──────────────────────┐
        │                      │                      │
 ┌──────┴───────┐      ┌──────┴───────┐     ┌───────┴────────┐
 │ LockManager  │      │  BufferMgr   │     │  LogManager    │
 └──────┬───────┘      └──────────────┘     └────────────────┘
        │
 ┌──────┴───────┐
 │  LockTable   │
 │  (Singleton) │
 └──────┬───────┘
        │
        ├─────────────┬─────────────┬──────────────────┐
        │             │             │                  │
 ┌──────┴───────┐ ┌──┴────┐  ┌────┴──────────┐  ┌───┴─────────────┐
 │WaitForGraph  │ │ Lock  │  │DeadlockDetector│  │ LockType enum   │
 │(optional)    │ │(Map)  │  │(Background)    │  │ IsolationLevel  │
 └──────────────┘ └───────┘  └────────────────┘  └─────────────────┘
```

### コンポーネント責務

| コンポーネント | 責務 |
|--------------|------|
| **IsolationLevel** | 分離レベルの定義（4種類）|
| **Tx** | トランザクション制御、分離レベル適用 |
| **LockManager** | トランザクション毎のロック追跡 |
| **LockTable** | グローバルなロック管理 |
| **Lock** | 単一ブロックのロック制御 |
| **WaitForGraph** | 待機関係の追跡、サイクル検出 |
| **DeadlockDetector** | 定期的なデッドロック検出と解決 |

---

## まとめ

### 実装完了機能

✅ **Wait-For Graph**
- トランザクション間の待機関係を有向グラフで表現
- DFSアルゴリズムによるサイクル検出

✅ **DeadlockDetector**
- 定期的な自動検出（設定可能な間隔）
- Victim選択（最大ID戦略）
- コールバックによる abort通知

✅ **IsolationLevel**
- 4つの標準分離レベル実装
- ロック戦略の自動切り替え
- Txクラスとの統合

✅ **分離レベルによる並行制御**
- READ_UNCOMMITTED: ロックなし
- READ_COMMITTED: 短期ロック（Dirty Read防止）
- REPEATABLE_READ: 長期ロック（Non-Repeatable Read防止）
- SERIALIZABLE: 範囲ロック対応（Phantom Read防止）

### テスト結果

- ✅ WaitForGraphTest: 11テスト成功
- ✅ DeadlockDetectorTest: 7テスト成功
- ✅ IsolationLevelTest: 11テスト成功
- ✅ デモプログラム実行成功

### 次のステップ（Phase 1 Week 3）

1. **TableScan への統合**: レコードレベルのロック
2. **追加テスト**: Phantom Read防止テスト
3. **パフォーマンス最適化**: ロック競合の削減
