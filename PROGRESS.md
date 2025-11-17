# MiniDB 実装進捗チェックリスト

最終更新: 2025-11-16

---

## ✅ Phase 0: 基礎実装（完了）

### Part 1: 基礎レイヤー

- [x] Chapter 3: Disk and File Management
  - [x] FileMgr
  - [x] Page
  - [x] BlockId
- [x] Chapter 4: Memory Management
  - [x] BufferMgr
  - [x] Buffer
  - [x] LogManager
- [x] Chapter 5: Transaction Management (基本)
  - [x] Tx (基本機能)
  - [x] WAL
  - [x] Commit/Rollback

### Part 2: レコード管理

- [x] Chapter 6: Record Management
  - [x] Schema
  - [x] Layout
  - [x] RecordPage
  - [x] TableFile
  - [x] TableScan
- [x] Chapter 7: Metadata Management
  - [x] MetadataManager
  - [x] システムカタログ (tblcat, fldcat, idxcat)

### Part 3: クエリ処理

- [x] Chapter 8: Query Processing (基本)
  - [x] Scan interface
  - [x] TableScan, SelectScan, ProjectScan
  - [x] ProductScan (JOIN基本)
- [x] Chapter 9: Parsing
  - [x] Lexer
  - [x] Parser
  - [x] Ast (DDL/DML/DQL)
- [x] Chapter 10: Planning (基本)
  - [x] Planner
  - [x] Index Scan 最適化

### Part 4: 高度な機能

- [x] Chapter 12: Indexing (基本)
  - [x] B+木インデックス
  - [x] 等値検索・範囲検索
- [x] Chapter 13-15: Materialization & Sorting (基本)
  - [x] OrderByScan (メモリ内)
  - [x] GroupByScan
  - [x] DistinctScan

---

## 🔄 Phase 1: 並行制御の完全実装（進行中）

**開始日**: 2025-11-16  
**期限**: 2025-12-07 (3週間)  
**担当者**: -  
**ブランチ**: `feature/phase1-locking`

### Week 1: ロック管理基盤 ✅ **完了** (2025-11-16)

- [x] `LockType` enum 作成
  - [x] コード実装 (`src/main/java/app/tx/lock/LockType.java`)
  - [x] Javadoc 追加
  - [x] SHARED/EXCLUSIVE の2種類
  - [x] `isCompatibleWith()` メソッドで互換性チェック
- [x] `Lock` クラス作成
  - [x] Shared lock 取得
  - [x] Exclusive lock 取得
  - [x] ロック解放
  - [x] 待機キュー管理 (FIFO)
  - [x] タイムアウト処理 (デフォルト10秒)
  - [x] ロックアップグレード対応
  - [x] 単体テスト (10テスト)
- [x] `LockTable` クラス作成
  - [x] グローバルロックテーブル
  - [x] スレッドセーフな実装 (`ConcurrentHashMap`)
  - [x] 単体テスト (6テスト)
- [x] `LockManager` クラス作成
  - [x] トランザクション毎のロック追跡
  - [x] ロック取得API (`sLock/xLock`)
  - [x] 一括解放 (`release`)
  - [x] 単体テスト (9テスト)
- [x] `LockAbortException` 作成
  - [x] タイムアウト時の例外処理
- [x] `Tx` クラスへの統合
  - [x] getInt/getString に sLock 追加
  - [x] setInt/setString に xLock 追加
  - [x] commit/rollback でロック解放
  - [x] 統合テスト
- [x] 並行トランザクションテスト
  - [x] Lost Update 防止テスト (5テスト)
  - [x] Dirty Read 防止テスト
  - [x] ロックアップグレードテスト
  - [x] 並行アクセステスト
- [x] デモプログラム作成
  - [x] `LockingDemo.java` 作成
  - [x] Lost Update 防止デモ
  - [x] Dirty Read 防止デモ
  - [x] 共有ロック（複数読み取り）デモ
  - [x] 実行確認 ✅
- [x] ドキュメント作成
  - [x] `docs/LOCKING_LOGIC_DIAGRAMS.md` 作成
  - [x] アーキテクチャ図
  - [x] シーケンス図
  - [x] 状態遷移図
  - [x] Strict 2PL プロトコル図
  - [x] 使用例とトラブルシューティング

**テスト結果**: 30テスト全て成功 ✅  
**コミット**: 完了 (feature/phase1-locking ブランチ)

### Week 2: デッドロック検出と分離レベル ✅ **完了** (2025-11-17)

- [x] Wait-For Graph 実装
  - [x] グラフ構築 (`WaitForGraph.java`)
  - [x] DFS サイクル検出アルゴリズム
  - [x] 単体テスト (11テスト)
- [x] `DeadlockDetector` クラス作成
  - [x] 周期的なデッドロック検出（バックグラウンドスレッド）
  - [x] Victim 選択（最大ID戦略）
  - [x] 自動ロールバック対応
  - [x] 単体テスト (7テスト)
- [x] `IsolationLevel` enum 作成
  - [x] READ_UNCOMMITTED
  - [x] READ_COMMITTED
  - [x] REPEATABLE_READ
  - [x] SERIALIZABLE
- [x] 各分離レベルの実装
  - [x] ロック保持期間の制御
  - [x] READ_COMMITTED: 短期ロック（読取後即座に解放）
  - [x] REPEATABLE_READ: 長期ロック（コミットまで保持）
  - [x] 述語ロック (SERIALIZABLE) フレームワーク準備完了
  - [x] 統合テスト (11テスト)
- [x] デモプログラム作成
  - [x] `DeadlockAndIsolationDemo.java` 作成
  - [x] 分離レベルのプロパティ表示
  - [x] Non-Repeatable Read の防止デモ
  - [x] 実行確認 ✅
- [x] ドキュメント作成
  - [x] `docs/PHASE1_WEEK2_DEADLOCK_AND_ISOLATION.md` 作成
  - [x] Wait-For Graph アーキテクチャ図
  - [x] DFS サイクル検出アルゴリズム
  - [x] 分離レベル比較表
  - [x] Non-Repeatable Read 防止タイムライン図

**テスト結果**: 29テスト全て成功 ✅ (全体59テスト合格)  
**コミット**: 準備完了 (feature/phase1-week2-deadlock-detection ブランチ)

### Week 3: TableScan統合とパフォーマンス

- [ ] `TableScan` へのロック統合
  - [ ] next() に sLock
  - [ ] insert/update/delete に xLock
  - [ ] テスト
- [ ] 追加の並行テスト
  - [ ] Non-repeatable Read テスト
  - [ ] Phantom Read テスト
- [ ] パフォーマンステスト
  - [ ] ロック競合下でのスループット
  - [ ] デッドロック検出のオーバーヘッド
- [ ] コードレビュー
  - [ ] PR 作成
  - [ ] レビュー対応
  - [ ] マージ

**完了率**: 73% (22/30タスク完了)

---

## ⏳ Phase 2: リカバリ機能の完全実装（未着手）

**予定開始日**: 2025-12-07  
**予定期限**: 2025-12-28 (3週間)

### Week 1: Recovery Manager基盤

- [ ] `RecoveryManager` クラス作成
- [ ] UNDO ログ拡張
- [ ] REDO ログ実装
- [ ] ログレコード型追加

### Week 2: チェックポイントとリカバリ

- [ ] `CheckpointManager` 実装
- [ ] クラッシュリカバリ処理
- [ ] フォワードリカバリ

### Week 3: 統合とテスト

- [ ] システム起動時リカバリ
- [ ] クラッシュシミュレーションテスト
- [ ] ドキュメント作成

**完了率**: 0% (0/12)

---

## ⏳ Phase 3: JDBC ドライバー実装（未着手）

**予定開始日**: 2025-12-28  
**予定期限**: 2026-01-25 (4週間)

### Week 1-2: 基本インターフェース

- [ ] `MiniDBDriver` 実装
- [ ] `MiniDBConnection` 実装
- [ ] `MiniDBStatement` 実装
- [ ] URL 解析

### Week 3: PreparedStatement と ResultSet

- [ ] `MiniDBPreparedStatement` 実装
- [ ] `MiniDBResultSet` 実装
- [ ] `MiniDBResultSetMetaData` 実装

### Week 4: メタデータと統合

- [ ] `MiniDBDatabaseMetaData` 実装
- [ ] トランザクション統合
- [ ] 統合テスト
- [ ] ドキュメント

**完了率**: 0% (0/16)

---

## ⏳ Phase 4: クエリ最適化の高度化（未着手）

**予定開始日**: 2026-01-25  
**予定期限**: 2026-02-22 (4週間)

### 統計情報

- [ ] `TableStats` 実装
- [ ] `IndexStats` 実装
- [ ] `Histogram` 実装
- [ ] ANALYZE コマンド

### コストモデル

- [ ] `CostEstimator` 実装
- [ ] `JoinOrderOptimizer` 実装
- [ ] Selectivity 推定

### 物理演算子拡張

- [ ] `MergeJoinScan` 実装
- [ ] `HashJoinScan` 実装
- [ ] コストベース選択

**完了率**: 0% (0/10)

---

## ⏳ Phase 5: スケーラビリティ向上（未着手）

**予定開始日**: 2026-02-22  
**予定期限**: 2026-03-29 (5週間)

### 外部ソート

- [ ] `ExternalSortScan` 実装
- [ ] 多段階マージソート
- [ ] テスト (100万行以上)

### 外部集約

- [ ] `ExternalGroupByScan` 実装
- [ ] `HashAggregation` 実装

### 複合インデックス

- [ ] 複数カラムキー対応
- [ ] 部分一致検索
- [ ] 文字列インデックス

**完了率**: 0% (0/8)

---

## ⏳ Phase 6: ネットワークサーバー（未着手）

**予定開始日**: 2026-03-29  
**予定期限**: 2026-05-03 (5週間)

### サーバー実装

- [ ] `MiniDBServer` 実装
- [ ] `ClientHandler` 実装
- [ ] プロトコル定義
- [ ] セッション管理

### クライアント

- [ ] `MiniDBClient` 実装
- [ ] JDBC リモート接続対応

### セキュリティ

- [ ] ユーザー認証
- [ ] パスワードハッシュ化
- [ ] SSL/TLS (オプション)

**完了率**: 0% (0/9)

---

## ⏳ Phase 7: 高度な SQL 機能（未着手）

**予定開始日**: 2026-05-03  
**予定期限**: 2026-05-31 (4週間)

### 追加 DDL

- [ ] ALTER TABLE
- [ ] CREATE VIEW
- [ ] PRIMARY KEY / FOREIGN KEY

### サブクエリ

- [ ] FROM サブクエリ
- [ ] WHERE IN サブクエリ
- [ ] EXISTS / NOT EXISTS
- [ ] スカラーサブクエリ

### 高度な述語

- [ ] OR/AND/NOT 複雑なネスト
- [ ] LIKE 'pattern%'
- [ ] IN (value1, value2, ...)
- [ ] CASE WHEN

### 集約関数拡張

- [ ] COUNT(DISTINCT col)
- [ ] STDDEV, VARIANCE
- [ ] MEDIAN

### ウィンドウ関数

- [ ] ROW_NUMBER()
- [ ] RANK()
- [ ] LAG/LEAD

**完了率**: 0% (0/18)

---

## ⏳ Phase 8: 運用機能（未着手）

**予定開始日**: 2026-05-31  
**予定期限**: 2026-06-21 (3週間)

### バックアップ・リストア

- [ ] `BackupManager` 実装
- [ ] `RestoreManager` 実装
- [ ] ホットバックアップ
- [ ] 増分バックアップ
- [ ] Point-in-Time Recovery

### 監視・診断

- [ ] `PerformanceMonitor` 実装
- [ ] `QueryProfiler` 実装
- [ ] システムビュー (sys.*)

### 管理コマンド

- [ ] ANALYZE TABLE
- [ ] VACUUM
- [ ] REINDEX
- [ ] EXPLAIN ANALYZE

### 設定管理

- [ ] `ConfigManager` 実装
- [ ] 設定ファイル読み込み

**完了率**: 0% (0/15)

---

## 🔵 Phase 9: パフォーマンスチューニング（継続的）

### バッファ管理改善

- [ ] LRU 置換アルゴリズム
- [ ] Clock 置換アルゴリズム
- [ ] ARC 実装
- [ ] 動的サイズ調整

### インデックス最適化

- [ ] B+木ファンアウト最適化
- [ ] Bulk Loading 最適化
- [ ] インデックス圧縮

### 並列処理

- [ ] 並列スキャン
- [ ] 並列結合
- [ ] パーティション並列

### キャッシュ戦略

- [ ] クエリ結果キャッシュ
- [ ] 実行プランキャッシュ
- [ ] メタデータキャッシュ

**完了率**: 0% (0/11)

---

## 🔵 Phase 10: テストとドキュメント（継続的）

### テストカバレッジ

- [ ] 単体テスト 90%以上
- [ ] 統合テスト全機能
- [ ] パフォーマンステスト
- [ ] ストレステスト

### ベンチマーク

- [ ] TPC-H ベンチマーク
- [ ] TPC-C ベンチマーク
- [ ] カスタムベンチマーク

### ドキュメント

- [ ] API Javadoc 完備
- [ ] アーキテクチャドキュメント
- [ ] ユーザーマニュアル
- [ ] 開発者ガイド
- [ ] パフォーマンスチューニングガイド

**完了率**: 30% (3/10)

---

## 📊 全体進捗サマリー

| Phase | ステータス | 完了率 | 期間 | 優先度 |
|-------|----------|--------|------|--------|
| Phase 0 | ✅ 完了 | 100% | - | - |
| Phase 1 | 🔄 進行中 | 0% | 3週間 | 🔴 最高 |
| Phase 2 | ⏳ 未着手 | 0% | 3週間 | 🔴 最高 |
| Phase 3 | ⏳ 未着手 | 0% | 4週間 | 🟠 高 |
| Phase 4 | ⏳ 未着手 | 0% | 4週間 | 🟠 高 |
| Phase 5 | ⏳ 未着手 | 0% | 5週間 | 🟡 中 |
| Phase 6 | ⏳ 未着手 | 0% | 5週間 | 🟡 中 |
| Phase 7 | ⏳ 未着手 | 0% | 4週間 | 🟢 低 |
| Phase 8 | ⏳ 未着手 | 0% | 3週間 | 🟢 低 |
| Phase 9 | 🔵 継続的 | 0% | 継続 | 🔵 継続 |
| Phase 10 | 🔵 継続的 | 30% | 継続 | 🔵 継続 |

**総合進捗**: 約45% (Phase 0 完了ベース)

---

## 🎯 今週のタスク（2025-11-16 〜 2025-11-23）

### Phase 1 Week 1 タスク

#### 月曜日 (11/18)

- [ ] `LockType` enum 作成
- [ ] `Lock` クラス スケルトン作成
- [ ] 基本的な shared lock 実装

#### 火曜日 (11/19)

- [ ] `Lock` クラス exclusive lock 実装
- [ ] 待機キューとタイムアウト実装
- [ ] `Lock` 単体テスト作成

#### 水曜日 (11/20)

- [ ] `LockTable` クラス作成
- [ ] スレッドセーフな実装
- [ ] `LockTable` 単体テスト

#### 木曜日 (11/21)

- [ ] `LockManager` クラス作成
- [ ] トランザクション毎のロック追跡
- [ ] `LockManager` 単体テスト

#### 金曜日 (11/22)

- [ ] `Tx` クラスへの統合開始
- [ ] getInt/getString に sLock 追加
- [ ] 統合テスト作成

#### 土日 (11/23-24)

- [ ] setInt/setString に xLock 追加
- [ ] commit/rollback でロック解放
- [ ] Week 1 完了確認

---

## 📝 備考・メモ

### 実装時の注意点

1. **Phase 1 (並行制御)**
   - デッドロック検出は慎重に実装
   - パフォーマンステストを十分に実施
   - 既存の Tx 実装との互換性維持

2. **Phase 2 (リカバリ)**
   - Phase 1 のロック機構と密接に連携
   - クラッシュシミュレーションテストが重要

3. **Phase 3 (JDBC)**
   - JDBC 仕様への準拠を重視
   - 既存の Planner/Tx との統合に注意

### 参考リソース

- [IMPLEMENTATION_PLAN.md](./IMPLEMENTATION_PLAN.md) - 全体プラン
- [docs/PHASE1_LOCKING.md](./docs/PHASE1_LOCKING.md) - Phase 1 詳細
- Database Design and Implementation (Second Edition) - 教科書

---

**最終更新**: 2025-11-16  
**次回更新予定**: 毎週日曜日
