package app.cli;

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
 * - 1行またはセミコロン区切りでSQLを受け付けて実行（SELECTのみ）
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

            // セミコロン終端 or 1行SQL
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
                        SQL:
                          - Enter a SELECT statement (1 line or end with ';')
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
        System.out.println("SQL> " + sql);

        Ast.SelectStmt ast;
        try {
            ast = new Parser(sql).parseSelect();
        } catch (Exception e) {
            System.out.println("Parse ERROR: " + e.getMessage());
            return;
        }

        // 予定カラムを推定（表示用）
        List<String> cols;
        try {
            cols = resolveOutputColumns(ast);
        } catch (Exception e) {
            System.out.println("Resolve-cols WARN: " + e.getMessage());
            cols = List.of("id", "name", "student_id", "score", "count", "sum_score", "avg_score", "min_score",
                    "max_score");
        }

        try (Scan s = planner.plan(sql)) {
            s.beforeFirst();
            TablePrinter tp = new TablePrinter(cols);
            int rows = 0;
            while (s.next()) {
                List<String> out = new ArrayList<>(cols.size());
                for (String c : cols)
                    out.add(read(s, c));
                tp.addRow(out);
                rows++;
            }
            tp.print();
            if (rows == 0)
                System.out.println("(0 rows)");
        } catch (Exception e) {
            System.out.println("Exec ERROR: " + e.getMessage());
        }
    }

    /** 表示カラム推定: SELECT list / 集約 / GROUP BY / SELECT * (単表/単純JOINのみ) を最小対応 */
    private List<String> resolveOutputColumns(Ast.SelectStmt ast) {
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
                    String n = strip(((Ast.SelectItem.Column) it).name);
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

    private static String read(Scan s, String field) {
        try {
            return Integer.toString(s.getInt(field));
        } catch (Exception ignore) {
        }
        try {
            return s.getString(field);
        } catch (Exception ignore) {
        }
        return "-";
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
                ts.insert();
                ts.setInt("id", 1);
                ts.setString("name", "Ada");
                ts.insert();
                ts.setInt("id", 2);
                ts.setString("name", "Ada");
                ts.insert();
                ts.setInt("id", 3);
                ts.setString("name", "Turing");
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
                ts.insert();
                ts.setInt("student_id", 1);
                ts.setInt("score", 70);
                ts.insert();
                ts.setInt("student_id", 1);
                ts.setInt("score", 90);
                ts.insert();
                ts.setInt("student_id", 2);
                ts.setInt("score", 60);
                ts.insert();
                ts.setInt("student_id", 2);
                ts.setInt("score", 80);
                ts.insert();
                ts.setInt("student_id", 2);
                ts.setInt("score", 100);
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
    }

    /** 超簡易テーブルプリンタ（等幅） */
    private static final class TablePrinter {
        private final List<String> headers;
        private final List<List<String>> rows = new ArrayList<>();

        TablePrinter(List<String> headers) {
            this.headers = headers;
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
}
