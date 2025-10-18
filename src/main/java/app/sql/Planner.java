package app.sql;

import app.index.IndexRegistry;
import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;
import app.query.BTreeEqScan;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class Planner {
    private final FileMgr fm;
    private final MetadataManager mdm;
    private final IndexRegistry idxReg;

    public Planner(FileMgr fm, MetadataManager mdm) {
        this(fm, mdm, null);
    }

    public Planner(FileMgr fm, MetadataManager mdm, IndexRegistry idxReg) {
        this.fm = fm;
        this.mdm = mdm;
        this.idxReg = idxReg;
    }

    public Scan plan(String sql) {
        Ast.SelectStmt ast = new Parser(sql).parseSelect();

        // === FROM ===
        Layout baseLayout = mdm.getLayout(ast.from.table);
        TableFile baseTf = new TableFile(fm, ast.from.table + ".tbl", baseLayout);
        Scan s = new TableScan(fm, baseTf);

        // === JOIN（既存の HashIndex 経路は温存）===
        for (Ast.Join j : ast.joins) {
            Layout rightLayout = mdm.getLayout(j.table);
            TableFile rightTf = new TableFile(fm, j.table + ".tbl", rightLayout);

            String leftCol = null, rightCol = null;
            if (j.on.left instanceof Ast.Expr.Col && j.on.right instanceof Ast.Expr.Col) {
                String l = ((Ast.Expr.Col) j.on.left).name;
                String r = ((Ast.Expr.Col) j.on.right).name;
                leftCol = stripQualifier(l);
                rightCol = stripQualifier(r);
            }

            boolean usedIndex = false;
            if (idxReg != null && rightCol != null) {
                Optional<app.index.HashIndex> opt = idxReg.findHashIndex(j.table, rightCol);
                if (opt.isPresent()) {
                    System.out.println("[PLAN] join using index on " + j.table + "." + rightCol);
                    s = new app.query.IndexJoinScan(
                            s, fm, rightLayout, j.table + ".tbl", opt.get(), leftCol, rightCol);
                    usedIndex = true;
                }
            }
            if (!usedIndex) {
                System.out.println("[PLAN] join via product + filter (no index)");
                s = new ProductScan(s, new TableScan(fm, rightTf));
                s = new SelectScan(s, toPredicate(j.on));
            }
        }

        // === WHERE ===
        // 1表かつ「col = int」の等値述語で、B-Tree が登録されていれば B-Tree 経路を使う
        boolean usedIndexForWhere = false;
        if (ast.joins.isEmpty()) {
            for (Ast.Predicate p : ast.where) {
                if (p.left instanceof Ast.Expr.Col col && p.right instanceof Ast.Expr.I val) {
                    String colName = stripQualifier(col.name);
                    // idxcat から (table,col) のインデックス名を引く（なければ empty）
                    Optional<String> idxNameOpt = mdm.findIndexOn(ast.from.table, colName);
                    if (idxNameOpt.isPresent()) {
                        System.out.println("[PLAN] where using BTree index (EQ) on "
                                + ast.from.table + "." + colName);
                        s = new BTreeEqScan(fm, mdm, ast.from.table, idxNameOpt.get(), val.v);
                        usedIndexForWhere = true;
                        continue;
                    }
                }
                // （BTree なし or 対応外）→ 従来どおりフィルタ
                s = new SelectScan(s, toPredicate(p));
            }
        } else {
            for (Ast.Predicate p : ast.where) {
                s = new SelectScan(s, toPredicate(p));
            }
        }
        if (!usedIndexForWhere && ast.where != null && !ast.where.isEmpty())
            System.out.println("[PLAN] where via scan filter");

        // === GROUP BY / AGGREGATION ===（既存そのまま）
        List<String> outputCols = new ArrayList<>();
        boolean hasAgg = ast.projections.stream().anyMatch(it -> it instanceof Ast.SelectItem.Agg);
        if (hasAgg || ast.groupBy != null) {
            String groupField = ast.groupBy != null ? stripQualifier(ast.groupBy) : null;

            List<GroupByScan.Spec> specs = new ArrayList<>();
            for (Ast.SelectItem it : ast.projections) {
                if (it instanceof Ast.SelectItem.Agg a) {
                    app.query.Agg agg = app.query.Agg.valueOf(a.func);
                    String arg = a.arg == null ? null : stripQualifier(a.arg);
                    specs.add(new GroupByScan.Spec(agg, arg));
                }
            }

            s = new GroupByScan(s, groupField, specs);
            System.out.println("[PLAN] group-by/agg applied"
                    + (groupField != null ? " (group: " + groupField + ")" : " (global)"));

            if (groupField != null)
                outputCols.add(groupField);
            for (var sp : specs)
                outputCols.add(sp.outName());
            if (!outputCols.isEmpty())
                s = new ProjectScan(s, outputCols);

            if (ast.having != null) {
                String outName = toAggOutName(ast.having.func, ast.having.arg);
                var op = switch (ast.having.op) {
                    case ">" -> app.query.HavingScan.Op.GT;
                    case ">=" -> app.query.HavingScan.Op.GE;
                    case "<" -> app.query.HavingScan.Op.LT;
                    case "<=" -> app.query.HavingScan.Op.LE;
                    case "=" -> app.query.HavingScan.Op.EQ;
                    default -> throw new IllegalArgumentException("unsupported HAVING op: " + ast.having.op);
                };
                s = new app.query.HavingScan(s, outName, op, ast.having.rhs);
            }
        } else {
            boolean hasStar = ast.projections.stream()
                    .anyMatch(it -> it instanceof Ast.SelectItem.Column
                            && "*".equals(((Ast.SelectItem.Column) it).name));
            if (!hasStar && !ast.projections.isEmpty()) {
                for (Ast.SelectItem it : ast.projections) {
                    if (it instanceof Ast.SelectItem.Column c) {
                        outputCols.add(stripQualifier(c.name));
                    }
                }
                if (!outputCols.isEmpty())
                    s = new ProjectScan(s, outputCols);
            } else {
                outputCols.clear();
            }
        }

        // DISTINCT / ORDER BY / LIMIT（既存そのまま）
        if (ast.distinct) {
            List<String> cols;
            if (outputCols.isEmpty()) {
                if (ast.groupBy != null || hasAgg)
                    cols = new java.util.ArrayList<>(outputCols);
                else
                    throw new IllegalArgumentException("DISTINCT with SELECT * is not supported in this minimal impl");
            } else
                cols = outputCols;
            s = new app.query.DistinctScan(s, cols);
        }

        if (ast.orderBy != null) {
            String fld = stripQualifier(ast.orderBy.field);
            List<String> carry = outputCols.isEmpty() ? List.of(fld) : outputCols;
            s = new OrderByScan(s, fld, ast.orderBy.asc, carry);
        }

        if (ast.limit != null)
            s = new LimitScan(s, ast.limit);
        return s;
    }

    // ===== B-Tree の等値スキャン（RangeCursor を介して実装） =====
    // static final class BTreeEqScan implements Scan {
    // private final FileMgr fm;
    // private final MetadataManager mdm;
    // private final String table;
    // private final String indexName;
    // private final int key;

    // private TableFile tf;
    // private TableScan ts;
    // private BTreeIndex idx;
    // private RangeCursor cur;

    // BTreeEqScan(FileMgr fm, MetadataManager mdm, String table, String indexName,
    // int key) {
    // this.fm = fm;
    // this.mdm = mdm;
    // this.table = table;
    // this.indexName = indexName;
    // this.key = key;
    // init(); // 初期化
    // }

    // private void init() {
    // try {
    // Layout layout = mdm.getLayout(table);
    // this.tf = new TableFile(fm, table + ".tbl", layout);
    // this.ts = new TableScan(fm, tf);
    // this.idx = new BTreeIndex(fm, indexName, table + ".tbl");
    // this.idx.open();
    // // 等値は [k,k] のレンジとして扱う
    // this.cur = idx.range(SearchKey.ofInt(key), true, SearchKey.ofInt(key), true);
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }

    // @Override
    // public void beforeFirst() {
    // // テーブル側を先頭に戻し、カーソル/インデックスも再作成
    // try {
    // if (cur != null)
    // cur.close();
    // } catch (Exception ignore) {
    // }
    // try {
    // if (idx != null)
    // idx.close();
    // } catch (Exception ignore) {
    // }
    // ts.beforeFirst();
    // init(); // 再初期化
    // }

    // @Override
    // public boolean next() {
    // try {
    // if (!cur.next())
    // return false;
    // RID r = cur.getDataRid();
    // return ts.moveTo(r);
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }

    // @Override
    // public int getInt(String col) {
    // return ts.getInt(col);
    // }

    // @Override
    // public String getString(String c) {
    // return ts.getString(c);
    // }

    // @Override
    // public void close() {
    // // Scan.close() は throws しない想定：内部で握りつぶす
    // try {
    // if (cur != null)
    // cur.close();
    // } catch (Exception ignore) {
    // }
    // try {
    // if (idx != null)
    // idx.close();
    // } catch (Exception ignore) {
    // }
    // try {
    // if (ts != null)
    // ts.close();
    // } catch (Exception ignore) {
    // }
    // // tf は close を持たないので何もしない
    // }
    // }

    private static String stripQualifier(String name) {
        return (name != null && name.contains(".")) ? name.substring(name.indexOf('.') + 1) : name;
    }

    private Predicate toPredicate(Ast.Predicate p) {
        if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.Col) {
            return Predicate.eqField(stripQualifier(((Ast.Expr.Col) p.left).name),
                    stripQualifier(((Ast.Expr.Col) p.right).name));
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.I) {
            return Predicate.eqInt(stripQualifier(((Ast.Expr.Col) p.left).name),
                    ((Ast.Expr.I) p.right).v);
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.S) {
            return Predicate.eqString(stripQualifier(((Ast.Expr.Col) p.left).name),
                    ((Ast.Expr.S) p.right).v);
        }
        throw new IllegalArgumentException("unsupported predicate");
    }

    private static String strip(String name) {
        return (name != null && name.contains(".")) ? name.substring(name.indexOf('.') + 1) : name;
    }

    private static String toAggOutName(String func, String arg) {
        String a = (arg == null) ? null : strip(arg);
        return switch (func) {
            case "COUNT" -> "count";
            case "SUM" -> "sum_" + a;
            case "AVG" -> "avg_" + a;
            case "MIN" -> "min_" + a;
            case "MAX" -> "max_" + a;
            default -> throw new IllegalArgumentException("unknown agg: " + func);
        };
    }
}
