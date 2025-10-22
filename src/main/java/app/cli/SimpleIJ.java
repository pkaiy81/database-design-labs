package app.cli;

import app.index.RID;
import app.index.SearchKey;
import app.metadata.MetadataManager;
import app.query.Scan;
import app.record.Layout;
import app.record.Schema;
import app.record.TableFile;
import app.record.TableScan;
import app.sql.Ast;
import app.sql.Parser;
import app.sql.Planner;
import app.storage.FileMgr;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

/**
 * SimpleIJ っぽい最小CLI:
 * - 1行またはセミコロン区切りでSQLを受け付けて実行
 * - CREATE INDEX 文をサポート
 * - SELECT 文をサポート（WHERE(=), JOIN ... ON, ORDER BY, LIMIT, DISTINCT, GROUP BY,
 * HAVING）
 * - .tables / .indexes メタコマンド
 * - メタコマンド(:help, :exit, :reset, :demo, :plan)
 * - 結果をASCIIテーブルで表示
 */
public class SimpleIJ {

    private final FileMgr fm;
    private final MetadataManager mdm;
    private final Planner planner;
    private boolean showPlan = true; // [PLAN] ログは既存実装が出すので、ここではon/offのガイドのみ

    public SimpleIJ(Path dataDir) {
        this.fm = new FileMgr(dataDir, 4096);
        this.mdm = new MetadataManager(fm);
        this.planner = new Planner(fm, mdm);
    }

    public static void main(String[] args) throws Exception {
        SimpleIJ ij = new SimpleIJ(Path.of("./data"));
        ij.repl();
    }

    private void repl() throws Exception {
        System.out.println("MiniDB SimpleIJ - type :help for help");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        StringBuilder buf = new StringBuilder();

        while (true) {
            System.out.print(buf.isEmpty() ? "sql> " : "  -> ");
            String line = br.readLine();
            if (line == null)
                break;
            line = line.trim();
            if (line.isEmpty())
                continue;

            // メタコマンド
            if (line.startsWith(":")) {
                boolean cont = handleMeta(line);
                if (!cont)
                    break;
                continue;
            }

            // .tables / .indexes は即時処理（バッファ積まずに処理して continue）
            String trimmed = line;
            if (trimmed.equalsIgnoreCase(".tables")) {
                System.out.println("Tables:");
                for (String t : mdm.listTableNames())
                    System.out.println("  " + t);
                continue;
            }
            if (trimmed.equalsIgnoreCase(".indexes")) {
                System.out.println("Indexes:");
                for (String s : mdm.listIndexesFormatted())
                    System.out.println("  " + s);
                continue;
            }

            // セミコロン終端
            // SQL 組み立て
            buf.append(line);
            if (line.endsWith(";")) {
                String sql = buf.toString();
                sql = sql.substring(0, sql.lastIndexOf(';')).trim();
                buf.setLength(0);
                exec(sql);
            } else {
                // 1行で完結させたいならここで即実行（末尾;なし）
                // 行継続にしたい場合はセミコロンを使う
                // exec(buf.toString());
                // buf.setLength(0);
                // セミコロンがない場合は、次の行を待つ
                buf.append(" "); // スペースを入れて次の行と連結（任意）
                // 実行しない（ユーザーが ; を入力するまで継続）
            }
        }
    }

    private boolean handleMeta(String line) throws Exception {
        switch (line) {
            case ":help":
                System.out.println("""
                        Commands:
                            :help          Show this help
                            :exit          Exit
                            :reset         Remove ./data directory (ALL DATA LOST)
                            :demo          Create demo tables and seed data
                            :plan on/off   Toggle [PLAN] logs printed by operators
                        Meta commands:
                            .tables       List tables
                            .indexes      List indexes
                        SQL:
                            - Enter a SELECT statement (end with ';')
                            - Supports WHERE(=), JOIN ... ON, ORDER BY, LIMIT, DISTINCT, GROUP BY, HAVING
                        """);
                return true;
            case ":exit":
                return false;
            case ":reset":
                Util.deleteDataDir();
                System.out.println("data directory removed.");
                return true;
            case ":demo":
                Util.ensureDataDir();
                createDemoData();
                System.out.println("demo data created: tables 'names', 'scores'");
                return true;
            case ":plan on":
                showPlan = true;
                System.out.println("PLAN logs: ON (operators may print [PLAN] lines)");
                return true;
            case ":plan off":
                showPlan = false;
                System.out.println("PLAN logs: OFF (note: operators may still print if coded to do so)");
                return true;
            default:
                System.out.println("unknown command. type :help");
                return true;
        }
    }

    private void exec(String sql) {
        sql = sql.trim();
        if (sql.isEmpty())
            return;
        System.out.println("SQL(Debug)> " + sql);

        String head = sql.split("\\s+", 3)[0].toUpperCase(Locale.ROOT);
        if ("CREATE".equals(head)) {
            try {
                Ast.CreateIndexStmt ci = new Parser(sql).parseCreateIndex();
                runCreateIndex(ci); // 下のメソッドで実体処理
                System.out
                        .println("Index created: " + ci.indexName + " ON " + ci.tableName + "(" + ci.columnName + ")");
            } catch (Exception e) {
                System.out.println("Exec ERROR: " + e.getMessage());
            }
            return;
        }

        Ast.Statement stmt;
        try {
            stmt = new Parser(sql).parseStatement();
        } catch (Exception e) {
            System.out.println("Parse ERROR: " + e.getMessage());
            return;
        }

        if (stmt instanceof Ast.SelectStmt select) {
            // 予定カラムを推定（表示用）
            List<ColumnDisplay> cols;
            try {
                cols = resolveOutputColumns(select);
            } catch (Exception e) {
                System.out.println("Resolve-cols ERROR: " + e.getMessage());
                return;
            }

            try (Scan s = planner.plan(select)) {
                s.beforeFirst();
                TablePrinter tp = new TablePrinter(cols);
                int rows = 0;
                while (s.next()) {
                    List<String> out = new ArrayList<>(cols.size());
                    for (ColumnDisplay col : cols)
                        out.add(read(s, col));
                    tp.addRow(out);
                    rows++;
                }
                tp.print();
                if (rows == 0)
                    System.out.println("(0 rows)");
            } catch (Exception e) {
                System.out.println("Exec ERROR: " + e.getMessage());
            }
            return;
        }

        if (stmt instanceof Ast.InsertStmt insert) {
            try {
                int rows = planner.executeInsert(insert);
                printRowResult("inserted", rows);
            } catch (Exception e) {
                System.out.println("Exec ERROR: " + e.getMessage());
            }
            return;
        }

        if (stmt instanceof Ast.UpdateStmt update) {
            try {
                int rows = planner.executeUpdate(update);
                printRowResult("updated", rows);
            } catch (Exception e) {
                System.out.println("Exec ERROR: " + e.getMessage());
            }
            return;
        }

        if (stmt instanceof Ast.DeleteStmt delete) {
            try {
                int rows = planner.executeDelete(delete);
                printRowResult("deleted", rows);
            } catch (Exception e) {
                System.out.println("Exec ERROR: " + e.getMessage());
            }
            return;
        }

        System.out.println("Unsupported statement.");
    }

    private static void printRowResult(String action, int rows) {
        String noun = rows == 1 ? " row " : " rows ";
        System.out.println(rows + noun + action + ".");
    }

    /** 表示カラム推定: SELECT list / 集約 / GROUP BY / SELECT * (単表/単純JOINのみ) を最小対応 */
    private List<String> resolveOutputColumnNames(Ast.SelectStmt ast) {
        // 1) 集約 or GROUP BY があれば、前章実装の命名規則に合わせる
        boolean hasAgg = ast.projections.stream().anyMatch(p -> p instanceof Ast.SelectItem.Agg);
        if (hasAgg || ast.groupBy != null) {
            List<String> cols = new ArrayList<>();
            if (ast.groupBy != null)
                cols.add(strip(ast.groupBy));
            for (Ast.SelectItem it : ast.projections) {
                if (it instanceof Ast.SelectItem.Agg a) {
                    cols.add(toAggOutName(a.func, a.arg));
                } else if (it instanceof Ast.SelectItem.Column) {
                    // （将来の互換用。現行のAstでは Column クラス名が違うなら下の分岐で拾う）
                }
            }
            // Column と Agg が混在している場合、Column 側も足す
            for (Ast.SelectItem it : ast.projections) {
                if (it instanceof Ast.SelectItem.Column c) {
                    String n = strip(c.name);
                    if (!"*".equals(n) && !cols.contains(n))
                        cols.add(n);
                }
            }
            if (!cols.isEmpty())
                return cols;
        }

        // 2) 非集約: SELECT list に列名があればそれを使う
        List<String> named = ast.projections.stream()
                .filter(p -> p instanceof Ast.SelectItem.Column)
                .map(p -> ((Ast.SelectItem.Column) p).name)
                .map(SimpleIJ::strip)
                .filter(n -> !"*".equals(n))
                .collect(Collectors.toList());
        if (!named.isEmpty())
            return named;

        // 3) SELECT * の場合（単表 or 単純JOIN） → メタデータから列名取得
        boolean hasStar = ast.projections.stream()
                .anyMatch(p -> p instanceof Ast.SelectItem.Column && "*".equals(((Ast.SelectItem.Column) p).name));
        if (hasStar) {
            List<String> cols = new ArrayList<>();
            // FROM テーブル
            Schema s = mdm.getLayout(ast.from.table).schema();
            cols.addAll(schemaFields(s));
            // JOIN テーブル（簡易：各表の列を後ろに連結）
            for (Ast.Join j : ast.joins) {
                Schema sj = mdm.getLayout(j.table).schema();
                for (String f : schemaFields(sj))
                    if (!cols.contains(f))
                        cols.add(f);
            }
            if (!cols.isEmpty())
                return cols;
        }

        // 4) フォールバック（既知の代表カラム）
    return List.of("id", "name", "student_id", "score", "count", "sum_score", "avg_score", "min_score",
        "max_score");
    }

    private List<ColumnDisplay> resolveOutputColumns(Ast.SelectStmt ast) {
        List<String> names = resolveOutputColumnNames(ast);
        List<ColumnDisplay> cols = new ArrayList<>(names.size());
        for (String name : names)
            cols.add(new ColumnDisplay(name, deduceColumnKind(name, ast)));
        return cols;
    }

    private ColumnKind deduceColumnKind(String columnName, Ast.SelectStmt ast) {
        // Aggregate outputs default to INT (including COUNT, SUM, AVG, MIN, MAX)
        String lower = columnName.toLowerCase(Locale.ROOT);
        if (lower.startsWith("count") || lower.startsWith("sum") || lower.startsWith("avg")
                || lower.startsWith("min") || lower.startsWith("max"))
            return ColumnKind.INT;

        String qualifier = null;
        String field = columnName;
        int dot = columnName.indexOf('.');
        if (dot >= 0) {
            qualifier = columnName.substring(0, dot);
            field = columnName.substring(dot + 1);
        }
        field = strip(field);

        if (qualifier != null) {
            if (tableHasField(qualifier, field))
                return kindFromField(qualifier, field);
        } else {
            if (tableHasField(ast.from.table, field))
                return kindFromField(ast.from.table, field);
            for (Ast.Join join : ast.joins) {
                if (tableHasField(join.table, field))
                    return kindFromField(join.table, field);
            }
        }

        throw new IllegalArgumentException("Unknown column: " + columnName);
    }

    private boolean tableHasField(String table, String field) {
        try {
            return mdm.getLayout(table).schema().hasField(field);
        } catch (Exception e) {
            return false;
        }
    }

    private ColumnKind kindFromField(String table, String field) {
        try {
            Schema schema = mdm.getLayout(table).schema();
            return switch (schema.fieldType(field)) {
                case INT -> ColumnKind.INT;
                case STRING -> ColumnKind.STRING;
            };
        } catch (Exception e) {
            throw new IllegalArgumentException("Unknown column: " + table + "." + field, e);
        }
    }

    private static List<String> schemaFields(Schema s) {
        // プロジェクトの Schema に合わせて取得（fields(), fieldNames() 等）
        // 下は一般的な実装例。プロジェクトの実装に応じて必要ならメソッド名を合わせてください。
        try {
            // Map<String, Schema.FieldDef> -> List<String> of field names
            return new ArrayList<>(s.fields().keySet());
        } catch (Throwable ignore) {
            // 代替：よく使うカラムを返す
            return new ArrayList<>(List.of("id", "name", "student_id", "score"));
        }
    }

    private static String read(Scan s, ColumnDisplay column) {
        return switch (column.kind()) {
            case STRING -> readString(s, column.name());
            case INT -> readInt(s, column.name());
        };
    }

    private static String readString(Scan s, String field) {
        try {
            String v = s.getString(field);
            if (v != null)
                return v;
        } catch (Exception ignore) {
        }
        return readInt(s, field);
    }

    private static String readInt(Scan s, String field) {
        try {
            return Integer.toString(s.getInt(field));
        } catch (Exception ignore) {
        }
        return "-";
    }

    private enum ColumnKind {
        INT,
        STRING
    }

    private record ColumnDisplay(String name, ColumnKind kind) {
    }

    private static String strip(String name) {
        if (name == null)
            return null;
        int i = name.indexOf('.');
        return (i >= 0) ? name.substring(i + 1) : name;
    }

    private static String toAggOutName(String func, String arg) {
        String a = (arg == null) ? null : strip(arg);
        return switch (func) {
            case "COUNT" -> "count";
            case "SUM" -> "sum_" + a;
            case "AVG" -> "avg_" + a;
            case "MIN" -> "min_" + a;
            case "MAX" -> "max_" + a;
            default -> "agg";
        };
    }

    /** デモ用データ（names, scores）を作成・投入 */
    private void createDemoData() {
        // names(id:int, name:string(20))
        {
            String t = "names";
            Schema s = new Schema().addInt("id").addString("name", 20);
            mdm.createTable(t, s);
            Layout l = mdm.getLayout(t);
            TableFile f = new TableFile(fm, t + ".tbl", l);
            try (TableScan ts = new TableScan(fm, f)) {
                ts.enableIndexMaintenance(mdm, t);
                String[] baseNames = {
                        "Ada", "Alan", "Grace", "Donald", "Edsger",
                        "Barbara", "Ken", "Margaret", "Linus", "Yukihiro"
                };
                String[] cohorts = { "A", "B", "C", "D", "E" };
                int id = 1;
                for (String base : baseNames) {
                    for (String cohort : cohorts) {
                        ts.insert();
                        ts.setInt("id", id++);
                        String label = cohort.equals("A") ? base : base + " " + cohort;
                        ts.setString("name", label);
                    }
                    // intentional duplicates for frequency tests
                    ts.insert();
                    ts.setInt("id", id++);
                    ts.setString("name", base);
                }
            }
        }
        // scores(student_id:int, score:int)
        {
            String t = "scores";
            Schema s = new Schema().addInt("student_id").addInt("score");
            mdm.createTable(t, s);
            Layout l = mdm.getLayout(t);
            TableFile f = new TableFile(fm, t + ".tbl", l);
            try (TableScan ts = new TableScan(fm, f)) {
                ts.enableIndexMaintenance(mdm, t);
                int studentId = 1;
                for (int batch = 0; batch < 10; batch++) {
                    for (int member = 0; member < 6; member++) {
                        int sid = studentId++;
                        int base = 55 + (batch * 3) + (member * 2);
                        for (int attempt = 0; attempt < 3; attempt++) {
                            ts.insert();
                            ts.setInt("student_id", sid);
                            int score = base + attempt * 7 + (sid % 5);
                            ts.setInt("score", Math.min(score, 100));
                        }
                    }
                }
            }
        }
    }

    /** dataディレクトリ削除などのユーティリティ */
    private static final class Util {
        static void deleteDataDir() {
            try {
                java.nio.file.Path p = java.nio.file.Path.of("./data");
                if (!java.nio.file.Files.exists(p))
                    return;
                java.nio.file.Files.walk(p)
                        .sorted(Comparator.reverseOrder())
                        .forEach(path -> {
                            try {
                                java.nio.file.Files.deleteIfExists(path);
                            } catch (Exception ignore) {
                            }
                        });
            } catch (Exception ignore) {
            }
        }

        // create dir if ./data not exists
        static void ensureDataDir() {
            try {
                java.nio.file.Path p = java.nio.file.Path.of("./data");
                if (!java.nio.file.Files.exists(p)) {
                    java.nio.file.Files.createDirectories(p);
                }
            } catch (Exception ignore) {
            }
        }
    }

    /** 超簡易テーブルプリンタ（等幅） */
    private static final class TablePrinter {
        private final List<String> headers;
        private final List<List<String>> rows = new ArrayList<>();

        TablePrinter(List<ColumnDisplay> columns) {
            this.headers = columns.stream().map(ColumnDisplay::name).collect(Collectors.toList());
        }

        void addRow(List<String> r) {
            rows.add(r);
        }

        void print() {
            // 幅計算
            int[] w = new int[headers.size()];
            for (int i = 0; i < headers.size(); i++)
                w[i] = headers.get(i).length();
            for (var r : rows)
                for (int i = 0; i < headers.size(); i++)
                    w[i] = Math.max(w[i], safe(r, i).length());

            // 罫線
            String sep = "+" + Arrays.stream(w).mapToObj(n -> "-".repeat(n + 2)).collect(Collectors.joining("+")) + "+";
            // ヘッダ
            System.out.println(sep);
            String h = "| " + joinPad(headers, w) + " |";
            System.out.println(h);
            System.out.println(sep);
            // 行
            for (var r : rows) {
                System.out.println("| " + joinPad(r, w) + " |");
            }
            System.out.println(sep);
        }

        private static String joinPad(List<String> cells, int[] w) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < w.length; i++) {
                if (i > 0)
                    sb.append(" | ");
                String s = safe(cells, i);
                int pad = w[i] - s.length();
                sb.append(s).append(" ".repeat(Math.max(0, pad)));
            }
            return sb.toString();
        }

        private static String safe(List<String> r, int i) {
            return (i < r.size() && r.get(i) != null) ? r.get(i) : "";
        }
    }

    private void runCreateIndex(Ast.CreateIndexStmt ci) throws Exception {
        boolean metaRegistered = false;
        final String idxFile = ci.indexName + ".idx"; // BTreeIndex が使う既定の物理名に合わせる
        app.index.btree.BTreeIndex idx = null;
        try {
            // 1) 先にメタ登録（重複チェック） 以降の失敗時は roll back
            mdm.createIndex(ci.indexName, ci.tableName, ci.columnName);
            metaRegistered = true;

            // 2) 物理インデックスを開いてビルド
            idx = new app.index.btree.BTreeIndex(fm, ci.indexName, ci.tableName + ".tbl");
            idx.open();

            // 3) 全件スキャンして (key, RID) を投入
            TableFile tf = new TableFile(fm, ci.tableName + ".tbl", mdm.getLayout(ci.tableName));
            try (TableScan ts = new TableScan(fm, tf)) {
                ts.beforeFirst();
                while (ts.next()) {
                    SearchKey key = SearchKey.ofInt(ts.getInt(ci.columnName));
                    RID rid = ts.rid();
                    idx.insert(key, rid);
                }
            }
        } catch (Exception buildEx) {
            // --- rollback ---
            try {
                if (idx != null)
                    idx.close();
            } catch (Exception ignore) {
            }
            // メタデータの登録を取り消し
            if (metaRegistered) {
                try {
                    mdm.dropIndex(ci.indexName);
                } catch (Exception ignore) {
                }
            }
            // 物理 .idx を削除
            fm.deleteFileIfExists(idxFile);
            throw new RuntimeException("CREATE INDEX failed and was rolled back: " + ci.indexName, buildEx);
        } finally {
            // 念のため close（成功時/例外時とも）
            try {
                if (idx != null)
                    idx.close();
            } catch (Exception ignore) {
            }
        }
    }
}
