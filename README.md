# database-design-labs

Reference: Database Design and Implementation Second Edition

---

生成先：

* Windows: `build/install/minidb/bin/minidb.bat`
* macOS/Linux: `build/install/minidb/bin/minidb`

#### 2) 起動

**Windows:**

```powershell
.\build\install\minidb\bin\minidb.bat
```

**macOS/Linux:**

```bash
./build/install/minidb/bin/minidb
```

### これは何？

Java（Gradle）で実装した **自作ミニ RDBMS** です。ページ管理、レコード/テーブル、メタデータカタログ、クエリプランナ、各種演算子（選択/射影/結合/集約/重複排除/並べ替え/制限）、**B+木インデックス**を備え、SQL DDL/DML/DQL を実行できます。
付属の **SimpleIJ 風 CLI** でインタラクティブに SQL を入力し、データベースを操作できます。

---

### ✨ できること（対応 SQL 機能）

#### 📊 **DDL（データ定義言語）**

* `CREATE TABLE <table> (<col> INT | <col> STRING(<n>), ...)`
  * テーブルの作成（INT / STRING 型対応）
  * システムカタログ（`tblcat`, `fldcat`）への自動登録
* `DROP TABLE <table>`
  * テーブルの削除
  * 関連インデックスとメタデータの自動クリーンアップ
  * 物理ファイルの削除
* `CREATE INDEX <index> ON <table>(<column>)`
  * B+木インデックスの作成
  * 既存データの自動インデックス構築
* `DROP INDEX <index>`
  * インデックスの削除
  * メタデータとファイルのクリーンアップ

#### 📝 **DML（データ操作言語）**

* `INSERT INTO <table>(<cols>) VALUES (<values>)`
  * レコードの挿入
  * インデックスの自動メンテナンス
* `UPDATE <table> SET <col>=<val>, ... WHERE <predicates>`
  * レコードの更新
  * インデックスの自動更新
* `DELETE FROM <table> WHERE <predicates>`
  * レコードの削除
  * インデックスからの自動削除

#### 🔍 **DQL（データ照会言語）**

* `SELECT ... FROM ...`
  * `WHERE`：比較演算子（`=`, `>`, `>=`, `<`, `<=`）、`BETWEEN`
  * `JOIN ... ON left = right`（内部等値結合、複数段可）
  * `ORDER BY <単一列> [ASC|DESC]`
  * `LIMIT <N>`
  * `DISTINCT <列リスト>`
  * `GROUP BY <単一列>`
    * 集約：`COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`
    * `HAVING <AGG>(<col|*>) <op> <int>`
* `EXPLAIN <SELECT ...>`
  * クエリ実行プランの表示

#### 🚀 **クエリ最適化**

* **B+木インデックスによる高速化**
  * `WHERE col = <value>` → Index Equality Scan
  * `WHERE col BETWEEN <lo> AND <hi>` → Index Range Scan
  * `WHERE col > <value>` / `col >= <value>` / `col < <value>` / `col <= <value>` → Index Range Scan
  * `ORDER BY <indexed-col>` → Index Order Scan（ソート不要）
  * `JOIN ... ON <indexed-col>` → Index Join Scan

#### 🛠️ **CLI メタコマンド**

* `.tables` — テーブル一覧表示
* `.indexes` — インデックス一覧表示
* `.schema <table>` — テーブル定義の表示（CREATE TABLE 文の再現）

> **制限**：`OR`, `NOT`, `LIKE`, 複合 `ORDER BY`/`GROUP BY`、`DISTINCT *`、サブクエリは未対応。トランザクション機能は最小限。

---

### 必要環境

* **JDK 17 以上**（`java -version` で確認）
* **Gradle Wrapper 同梱**（`./gradlew` / `gradlew.bat` 使用。別途インストール不要）

---

## 🚀 クイックスタート

### 方法1: インストール型起動（推奨）

Gradle の Application Plugin で **自己完結の起動スクリプト**を生成し、そこから CLI を起動します。

#### 1) 起動スクリプトを生成

```bash
# Windows PowerShell / macOS / Linux 共通
./gradlew installDist
```

生成先：

* Windows: `build/install/minidb/bin/minidb.bat`
* macOS/Linux: `build/install/minidb/bin/minidb`

#### 2) 起動

**Windows:**

```powershell
.\build\install\minidb\bin\minidb.bat
```

**macOS/Linux:**

```bash
./build/install/minidb/bin/minidb
```

### 方法2: Gradle run での起動

```bash
./gradlew run
```

---

## 💡 使用例

### 基本セッション

```sql
MiniDB SimpleIJ - type :help for help
sql> :help
Commands:
    :help          Show this help
    :exit          Exit
    :reset         Remove ./data directory (ALL DATA LOST)
    :demo          Create demo tables and seed data
    :plan on/off   Toggle [PLAN] logs printed by operators
Meta commands:
    .tables       List tables
    .indexes      List indexes
    .schema <tbl> Show CREATE TABLE statement
SQL:
    - Enter SQL statements (end with ';')
    - Supports DDL, DML, and DQL

sql> :demo
demo data created: tables 'names', 'scores'

sql> .tables
Tables:
  names
  scores

sql> .schema names
CREATE TABLE names (
  id INT,
  name STRING(20)
);

sql> SELECT * FROM names WHERE id <= 2;
SQL(Debug)> SELECT * FROM names WHERE id <= 2
[PLAN] where via scan filter
+----+-------+
| id | name  |
+----+-------+
| 1  | Ada A |
| 2  | Ada B |
+----+-------+

sql> :exit
```

### DDL（テーブルとインデックスの作成）

```sql
-- テーブル作成
sql> CREATE TABLE people (
  ->   id INT,
  ->   name STRING(50),
  ->   age INT
  -> );
Table created: people

-- テーブル一覧
sql> .tables
Tables:
  people
  names
  scores

-- テーブル定義確認
sql> .schema people
CREATE TABLE people (
  id INT,
  name STRING(50),
  age INT
);

-- インデックス作成
sql> CREATE INDEX idx_people_id ON people(id);
Index created: idx_people_id ON people(id)

-- インデックス一覧
sql> .indexes
Indexes:
  idx_people_id ON people(id)

-- インデックス削除
sql> DROP INDEX idx_people_id;
Index dropped: idx_people_id

-- テーブル削除（関連インデックスも自動削除）
sql> DROP TABLE people;
Table dropped: people
```

### DML（データ操作）

```sql
-- データ挿入
sql> INSERT INTO people(id, name, age) VALUES (1, 'Alice', 25);
1 row inserted.

sql> INSERT INTO people(id, name, age) VALUES (2, 'Bob', 30);
1 row inserted.

sql> INSERT INTO people(id, name, age) VALUES (3, 'Charlie', 35);
1 row inserted.

-- データ照会
sql> SELECT * FROM people;
+----+---------+-----+
| id | name    | age |
+----+---------+-----+
| 1  | Alice   | 25  |
| 2  | Bob     | 30  |
| 3  | Charlie | 35  |
+----+---------+-----+

-- データ更新
sql> UPDATE people SET age = 26 WHERE id = 1;
1 row updated.

-- データ削除
sql> DELETE FROM people WHERE id = 3;
1 row deleted.

sql> SELECT * FROM people;
+----+-------+-----+
| id | name  | age |
+----+-------+-----+
| 1  | Alice | 26  |
| 2  | Bob   | 30  |
+----+-------+-----+
```

### DQL（高度なクエリ）

```sql
-- 比較演算子
sql> SELECT * FROM scores WHERE score > 80;
sql> SELECT * FROM scores WHERE score BETWEEN 70 AND 90;

-- 並べ替え
sql> SELECT * FROM scores ORDER BY score DESC LIMIT 10;
[PLAN] order-by via BTree index on scores.score (limit 10)
+------------+-------+
| student_id | score |
+------------+-------+
| 60         | 100   |
| 59         | 100   |
| 58         | 99    |
...

-- 集約
sql> SELECT student_id, COUNT(*), AVG(score) 
  -> FROM scores 
  -> GROUP BY student_id 
  -> HAVING COUNT(*) > 2;
[PLAN] group-by/agg applied (group: student_id)
+------------+-------+-----------+
| student_id | count | avg_score |
+------------+-------+-----------+
| 1          | 3     | 67        |
| 2          | 3     | 70        |
...

-- 結合
sql> SELECT n.name, s.score 
  -> FROM names n 
  -> JOIN scores s ON n.id = s.student_id 
  -> WHERE s.score > 90;

-- クエリプラン確認
sql> EXPLAIN SELECT * FROM scores WHERE score > 80 ORDER BY score;
IndexOrderScan (table=scores, index=idx_scores_score, order=ASC)
└─ Range (lo=80, hi=+, loInclusive=false)
```

### インデックスによる最適化

```sql
-- インデックス作成前
sql> SELECT * FROM scores WHERE score = 85;
[PLAN] where via scan filter
... (全件スキャン)

-- インデックス作成
sql> CREATE INDEX idx_scores_score ON scores(score);
Index created: idx_scores_score ON scores(score)

-- インデックス作成後（自動的にインデックスを使用）
sql> SELECT * FROM scores WHERE score = 85;
[PLAN] where using BTree index (EQ) on scores.score
... (高速インデックス検索)

-- 範囲検索もインデックス使用
sql> SELECT * FROM scores WHERE score >= 80 AND score <= 90;
[PLAN] where using BTree index (RANGE) on scores.score
...

-- ORDER BY もインデックス使用（ソート不要）
sql> SELECT * FROM scores ORDER BY score LIMIT 10;
[PLAN] order-by via BTree index on scores.score (limit 10)
...
```

---

## デモ（コードからテーブル作成→投入→実行）

学習用の固定デモも含めています。`build.gradle.kts` の `mainClass` を切り替えれば、従来通り Gradle の `run` で実行可能です。

```kotlin
application {
    // mainClass.set("app.example.GroupByDemo")         // 集約/グループ化のデモ
    // mainClass.set("app.example.DistinctHavingDemo")  // DISTINCT/HAVING のデモ
    mainClass.set("app.cli.SimpleIJ")                   // ← ふだんは CLI を推奨
}
```

実行：

```bash
./gradlew run
```

---

## 🏗️ アーキテクチャ

### システム構成

```text
┌─────────────────────────────────────────────────────────┐
│                     SimpleIJ (CLI)                      │
│                 Interactive SQL Interpreter             │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                 Parser & Planner                        │
│         Analysis SQL → AST → Query Plan Construction    │
└────────────┬───────────────────────┬────────────────────┘
             │                       │
    ┌────────▼────────┐     ┌────────▼───────────────────┐
    │ MetadataManager │     │  Query Operators           │
    │                 │     │  (Implementations of Scan) │
    └────────┬────────┘     └────────┬───────────────────┘
             │                       │
    ┌────────▼───────────────────────▼──────────┐
    │           TableScan & BTreeIndex          │
    │        Access record & Index              │
    └────────┬──────────────────────────────────┘
             │
    ┌────────▼───────────────────────┐
    │    FileMgr                     │
    │ Page Management / Persistence  │
    └────────────────────────────────┘
```

### 主要コンポーネント

* **FileMgr**: 固定長ブロック（ページ）の読み書き
* **TableFile / TableScan**: レコード単位のアクセス
* **BTreeIndex**: B+木インデックスの実装
* **MetadataManager**: システムカタログ（`tblcat`, `fldcat`, `idxcat`）の管理
* **Parser**: SQL → AST への変換
* **Planner**: AST → クエリプラン（Scan 木）の構築、最適化
* **Query Operators**: SelectScan, ProjectScan, JoinScan, OrderByScan など

---

## 📂 ディレクトリ構成

```text
.
├── build.gradle.kts           # ビルド設定
├── settings.gradle.kts
├── gradle/
│   └── wrapper/               # Gradle Wrapper
├── gradlew / gradlew.bat     # Gradle 実行スクリプト
├── build/
│   └── install/minidb/bin/   # ← 起動スクリプト生成先
├── data/                      # データベースファイル（実行時生成）
│   ├── tblcat.tbl            # テーブルカタログ
│   ├── fldcat.tbl            # フィールドカタログ
│   ├── idxcat.tbl            # インデックスカタログ
│   ├── <table>.tbl           # ユーザーテーブル
│   └── <index>               # B+木インデックスファイル
├── scripts/
│   └── reset-data.sh         # データ初期化スクリプト（任意）
└── src/
    ├── main/java/app/
    │   ├── storage/          # FileMgr, Page, BlockId
    │   ├── record/           # Schema, Layout, TableFile, TableScan
    │   ├── metadata/         # MetadataManager
    │   ├── index/            # BTreeIndex, SearchKey, RID
    │   ├── query/            # 各種 Scan 実装
    │   ├── sql/              # Lexer, Parser, Ast, Planner
    │   ├── cli/              # SimpleIJ（対話 CLI）
    │   └── example/          # デモプログラム群
    └── test/java/app/        # JUnit テスト
```

---

## 🧪 テスト

### テスト実行

```bash
./gradlew test
```

### テストレポート確認

```bash
# テスト完了後、HTMLレポートを開く
# Windows
start build/reports/tests/test/index.html
# macOS
open build/reports/tests/test/index.html
# Linux
xdg-open build/reports/tests/test/index.html
```

---

## 🛠️ データ初期化スクリプト

### macOS/Linux: `scripts/reset-data.sh`

```bash
#!/usr/bin/env bash
set -eu
rm -rf "$(dirname "$0")/../data"
echo "data directory removed."
```

実行：

```bash
chmod +x scripts/reset-data.sh
./scripts/reset-data.sh
```

### Windows PowerShell: `scripts/reset-data.ps1`

```powershell
$ErrorActionPreference = "Stop"
Remove-Item -Recurse -Force "$PSScriptRoot\..\data" -ErrorAction SilentlyContinue
Write-Host "data directory removed."
```

実行：

```powershell
.\scripts\reset-data.ps1
```

---

## ⚠️ 既知の制限 / 今後の拡張候補

### 現在の制限

* 述語は `=`, `>`, `>=`, `<`, `<=`, `BETWEEN` のみ（`OR`, `NOT`, `LIKE`, `IN` 未対応）
* `ORDER BY` / `GROUP BY` は **単一列のみ**
* `DISTINCT` は列指定のみ（`DISTINCT *` 未対応）
* サブクエリ未対応
* 外部結合（LEFT/RIGHT/FULL OUTER JOIN）未対応
* トランザクション機能は最小限（ロック、リカバリなし）
* メモリ内で完結（大規模データでは外部ソート/外部集約が必要）

### 今後の拡張候補

* [ ] `ALTER TABLE` サポート
* [ ] 複合インデックス
* [ ] 文字列カラムへのインデックス対応
* [ ] トランザクション（ACID）の完全実装
* [ ] ロギング & リカバリ
* [ ] 統計情報によるコストベース最適化
* [ ] マテリアライズドビュー

---

## 🐛 トラブルシュート

### CLI が即終了する

**原因**: `./gradlew run` で標準入力が正しく接続されていない

**解決策**: インストール型スクリプト（`build/install/minidb/bin/minidb`）から起動してください

または `build.gradle.kts` に以下を追加：

```kotlin
tasks.named<JavaExec>("run") {
    standardInput = System.`in`
}
```

### 前回データに引きずられる

**原因**: `./data` ディレクトリに古いデータが残っている

**解決策**: データディレクトリを削除

```bash
# macOS/Linux
rm -rf ./data

# Windows PowerShell
Remove-Item -Recurse -Force .\data
```

または CLI から `:reset` コマンドを実行

### パース エラーが出る

**原因**: SQL 構文の間違い、または未対応の構文

**確認事項**:

* SQL 文の末尾に `;` がついているか
* 対応している構文か（上記「できること」参照）
* テーブル名、カラム名が存在するか（`.tables`, `.schema` で確認）

### インデックスが使われない

**原因**: 条件に合致しないクエリ、または統計情報不足

**確認方法**: `EXPLAIN` でクエリプランを確認

```sql
sql> EXPLAIN SELECT * FROM scores WHERE score = 85;
```

**対処法**:

* インデックスが作成されているか `.indexes` で確認
* WHERE 条件がインデックスキーと一致しているか確認
* 必要に応じてインデックスを再作成

---

## 📚 参考文献

* Database Design and Implementation (Second Edition) by Edward Sciore
* SQLite Documentation
* PostgreSQL Documentation

---

## 📄 ライセンス

This project is for educational purposes.

---

## 👨‍💻 開発メモ

### バージョニング

* Current: `v0.16.0`（DDL/DML/DQL 完全対応、B+木インデックス）
* ブランチ: `feature/create-drop-table`

### 最近の更新
