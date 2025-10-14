# database-design-labs

Reference: Database Design and Implementation Second Edition

---

## MiniRDB — README（最終版 / インストール起動対応）

### これは何？

Java（Gradle）で実装した **自作ミニRDB** です。ページ管理、レコード/テーブル、メタデータ、簡易プランナ、演算子（選択/射影/結合/集約/重複排除/並べ替え/制限）、ハッシュ索引を備え、**一部の SQL** を実行できます。
付属の **SimpleIJ 風 CLI** でインタラクティブに SQL を入力できます。

---

### できること（対応 SQL 機能）

* `SELECT ... FROM ...`

  * `WHERE`：等値（`col = 123` / `col = 'abc'`）
  * `JOIN ... ON left = right`（内部等値結合、複数段可）
  * `ORDER BY <単一列> [ASC|DESC]`
  * `LIMIT <N>`
  * `DISTINCT <列リスト>`（`*` は未対応）
  * `GROUP BY <単一列>`

    * 集約：`COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`
    * `HAVING <AGG>(<col|*>) <op> <int>`（`> >= < <= =` の単一条件）
* **索引最適化（任意）**

  * 単表 `WHERE col = int` に **ハッシュ索引**があれば `IndexSelectScan`
  * 結合右側列に索引があれば **インデックス結合**（`IndexJoinScan`）

> 制限：`INSERT/UPDATE/DELETE`/`CREATE TABLE` の **SQL 文は未実装**（テーブル作成とデータ投入は Java の初期化コード/CLI の `:demo` を使用）。`OR/<>/LIKE/BETWEEN`、複合 `ORDER BY`/`GROUP BY`、`DISTINCT *` は未対応。

---

### 必要環境

* **JDK 17**（`java -version` で 17 系を確認）
* **Gradle Wrapper 同梱**（`./gradlew`／`gradlew.bat` 使用。別途インストール不要）

---

## クイックスタート（推奨：インストール型起動）

> Gradle の Application Plugin で **自己完結の起動スクリプト**（Windows: `.bat` / macOS・Linux: シェル）を生成し、そこから CLI を起動します。
> 標準入力の取り回し問題を避けられるため、**この方法を推奨**します。

### 1) 実行エントリとアプリ名を設定（`build.gradle.kts`）

```kotlin
application {
    mainClass.set("app.cli.SimpleIJ") // SimpleIJ 風CLIを既定のエントリに
    applicationName = "minidb"        // スクリプト名（任意、無指定ならプロジェクト名）
}
```

### 2) 起動スクリプトを生成

```bash
# Windows PowerShell / macOS / Linux 共通
./gradlew installDist
```

* 生成先：

  * Windows: `build/install/minidb/bin/minidb.bat`
  * macOS/Linux: `build/install/minidb/bin/minidb`

> ※ `applicationName` を変えた場合は該当名になります。未設定の場合は `settings.gradle.kts` の `rootProject.name` に基づく名称になります。

### 3) （任意）既存データの初期化

```bash
# macOS/Linux
rm -rf ./data
# Windows PowerShell
Remove-Item -Recurse -Force .\data
```

### 4) 起動

* Windows:

  ```powershell
  .\build\install\minidb\bin\minidb.bat
  ```
  
* macOS/Linux:

  ```bash
  ./build/install/minidb/bin/minidb
  ```

#### セッション例

```bash
MiniDB SimpleIJ - type :help for help
sql> :demo
demo data created: tables 'names', 'scores'
sql> SELECT DISTINCT name FROM names ORDER BY name;
+-------+
| name  |
+-------+
| Ada   |
| Turing|
+-------+
sql> :exit
```

> （補足）`./gradlew run` から直接起動する場合は、`build.gradle.kts` に次を追加すれば対話可能です：
>
> ```kotlin
> tasks.named<JavaExec>("run") {
>     standardInput = System.`in`
> }
> ```
>
> ただし、インストール型のほうが安定して入力を扱えます。

---

## CLI（SimpleIJ 風）の使い方

* **メタコマンド**

  * `:help` … ヘルプ
  * `:exit` … 終了
  * `:reset` … `./data` を削除（全データ消去）
  * `:demo` … サンプル表 `names` / `scores` を作成・投入
  * `:plan on|off` … [PLAN] ログ表示のガイド（演算子側の出力想定。必要なら連動に拡張可）
* **SQL 入力**

  * `SQL` 文を実行する場合には**末尾に `;`** を付けて実行
  * 対応句：`WHERE(=)`, `JOIN ... ON`, `ORDER BY`, `LIMIT`, `DISTINCT`, `GROUP BY`, `HAVING`

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

期待出力（DistinctHavingDemo の例）

```bash
SQL> SELECT DISTINCT name FROM names ORDER BY name
  row1: name=Ada, student_id=-, count=-, sum=-
  row2: name=Turing, student_id=-, count=-, sum=-
SQL> SELECT student_id, COUNT(*) FROM scores GROUP BY student_id HAVING COUNT(*) >= 2 ORDER BY student_id
[PLAN] group-by/agg applied (group: student_id)
  row1: name=-, student_id=1, count=2, sum=-
  row2: name=-, student_id=2, count=3, sum=-
SQL> SELECT SUM(score) FROM scores HAVING SUM(score) > 200
[PLAN] group-by/agg applied (global)
  row1: name=-, student_id=-, count=-, sum=400
```

---

## データ初期化スクリプト（任意）

`./scripts` に置くと便利です。

### macOS/Linux：`scripts/reset-data.sh`

```bash
#!/usr/bin/env bash
set -eu
rm -rf "$(dirname "$0")/../data"
echo "data directory removed."
```

### Windows PowerShell：`scripts/reset-data.ps1`

```powershell
$ErrorActionPreference = "Stop"
Remove-Item -Recurse -Force "$PSScriptRoot\..\data" -ErrorAction SilentlyContinue
Write-Host "data directory removed."
```

実行：

```bash
bash scripts/reset-data.sh    # macOS/Linux
pwsh scripts/reset-data.ps1   # Windows
```

---

## ディレクトリ構成（例）

```bash
.
├── build.gradle.kts
├── gradle/ / gradlew*            # Gradle Wrapper
├── build/install/minidb/bin/     # ← ここに起動スクリプトが生成される
├── data/                          # 実データ（実行時生成）
├── scripts/                       # reset-data.*（任意）
└── src/main/java/app
    ├── storage/                   # FileMgr, Page, BlockId
    ├── record/                    # Schema, Layout, TableFile, TableScan
    ├── metadata/                  # MetadataManager
    ├── index/                     # HashIndex
    ├── query/                     # 各 Scan 実装
    ├── sql/                       # Lexer/Parser/Ast/Planner
    └── cli/                       # SimpleIJ（対話 CLI）
       └── SimpleIJ.java
```

---

## 既知の制限 / 今後の拡張

* SQL での **DDL/DML**（`CREATE/INSERT/UPDATE/DELETE`）は未実装（Java API/CLI の `:demo` で代替）
* 述語は等値のみ。`OR/<>/LIKE/BETWEEN` は未対応
* `ORDER BY` / `GROUP BY` は **単一列のみ**
* メモリ内で完結（大規模データでは外部ソート/外部集約が必要）
* トランザクション/ロック/リカバリは最小限

---

## トラブルシュート

* **CLI が即終了する**
  → `./gradlew run` ではなく、**インストール型スクリプト**（`build/install/minidb/bin/minidb(.bat)`）から起動してください。
  → どうしても `run` を使う場合は `build.gradle.kts` に `standardInput = System.in` を設定。
* **前回データに引きずられる**
  → `./data` を削除（`scripts/reset-data.*` を利用）
* **`DISTINCT name` が数字に見える**
  → `DistinctScan` は **文字列優先**で値取得（本リポ最終版は修正済み）
* **`GROUP BY` + `ORDER BY` で `-` が出る**
  → `OrderByScan`/`GroupByScan` の **型優先（int⇄string）** 実装が入っているか確認（最終版OK）

---

## バージョニング（予定）

* タグ：`v0.15.0`（第15章までの完成版）
* ブランチ：`release/v0.15.0`
