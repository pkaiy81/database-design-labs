package app.sql;

import app.index.IndexRegistry;
import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;

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

        // === JOIN ===
        for (Ast.Join j : ast.joins) {
            Layout rightLayout = mdm.getLayout(j.table);
            TableFile rightTf = new TableFile(fm, j.table + ".tbl", rightLayout);

            // ON 左=右 を抽出（修飾名は末尾に正規化）
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
        // （1表かつ 等値(=int) かつ 索引あり の場合のみ IndexSelectScan に置換）
        boolean usedIndexForWhere = false;
        if (ast.joins.isEmpty() && idxReg != null) {
            for (Ast.Predicate p : ast.where) {
                if (p.left instanceof Ast.Expr.Col col && p.right instanceof Ast.Expr.I val) {
                    String colName = stripQualifier(col.name);
                    Optional<app.index.HashIndex> oidx = idxReg.findHashIndex(ast.from.table, colName);
                    if (oidx.isPresent()) {
                        System.out.println("[PLAN] where using index on " + ast.from.table + "." + colName);
                        s = new IndexSelectScan(fm, baseLayout, ast.from.table + ".tbl", oidx.get(), val.v);
                        usedIndexForWhere = true;
                    } else {
                        s = new SelectScan(s, toPredicate(p));
                    }
                } else {
                    s = new SelectScan(s, toPredicate(p));
                }
            }
        } else {
            for (Ast.Predicate p : ast.where) {
                s = new SelectScan(s, toPredicate(p));
            }
        }
        if (!usedIndexForWhere && ast.where != null && !ast.where.isEmpty())
            System.out.println("[PLAN] where via scan filter");

        // === GROUP BY / AGGREGATION ===
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

            // 集約結果の見える列
            if (groupField != null)
                outputCols.add(groupField);
            for (var sp : specs)
                outputCols.add(sp.outName());

            // 必要列だけに整形（*は簡易非対応のまま）
            if (!outputCols.isEmpty()) {
                s = new ProjectScan(s, outputCols);
            }
        } else {
            // 非集約時の PROJECTION
            boolean hasStar = ast.projections.stream()
                    .anyMatch(it -> it instanceof Ast.SelectItem.Column
                            && "*".equals(((Ast.SelectItem.Column) it).name));
            if (!hasStar && !ast.projections.isEmpty()) {
                for (Ast.SelectItem it : ast.projections) {
                    if (it instanceof Ast.SelectItem.Column c) {
                        outputCols.add(stripQualifier(c.name));
                    }
                }
                if (!outputCols.isEmpty()) {
                    s = new ProjectScan(s, outputCols);
                }
            } else {
                // SELECT * のときは、ORDER BY 用に最低限 並べ替えキーを後で carry します
                outputCols.clear();
            }
        }

        // === ORDER BY（単一列）===
        if (ast.orderBy != null) {
            String fld = stripQualifier(ast.orderBy.field);
            // carryFields: 既に決まっている出力列。未指定（SELECT * 等）の場合はキーだけでもOK
            List<String> carry = outputCols.isEmpty() ? List.of(fld) : outputCols;
            s = new OrderByScan(s, fld, ast.orderBy.asc, carry);
        }

        // === LIMIT ===
        if (ast.limit != null) {
            s = new LimitScan(s, ast.limit);
        }

        return s;
    }

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
}
