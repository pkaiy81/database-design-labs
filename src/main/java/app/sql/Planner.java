package app.sql;

import app.index.IndexRegistry;
import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;

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

        // FROM
        Layout baseLayout = mdm.getLayout(ast.from.table);
        TableFile baseTf = new TableFile(fm, ast.from.table + ".tbl", baseLayout);
        Scan s = new TableScan(fm, baseTf);

        // JOIN
        for (Ast.Join j : ast.joins) {
            Layout rightLayout = mdm.getLayout(j.table);
            TableFile rightTf = new TableFile(fm, j.table + ".tbl", rightLayout);

            // ON 左=右 を抽出（列名。修飾があれば末尾名を使用）
            String leftCol = null, rightCol = null;
            if (j.on.left instanceof Ast.Expr.Col && j.on.right instanceof Ast.Expr.Col) {
                String l = ((Ast.Expr.Col) j.on.left).name;
                String r = ((Ast.Expr.Col) j.on.right).name;
                // 右表の列がどちらかを判定（単純に列名一致で右表に存在すると仮定／実用ではメタデータで精査）
                // ここでは「右側の識別子が rightTable 側」とみなす
                rightCol = r.contains(".") ? r.substring(r.indexOf('.') + 1) : r;
                leftCol = l.contains(".") ? l.substring(l.indexOf('.') + 1) : l;
            }

            boolean usedIndex = false;
            if (idxReg != null && rightCol != null) {
                System.out.println("[PLAN] join with index");
                var opt = idxReg.findHashIndex(j.table, rightCol);
                if (opt.isPresent()) {
                    // 索引付き結合
                    s = new app.query.IndexJoinScan(s, fm, rightLayout, j.table + ".tbl", opt.get(), leftCol, rightCol);
                    usedIndex = true;
                }
            }
            if (!usedIndex) {
                System.out.println("[PLAN] join without index");
                // フォールバック：直積＋等値選択
                s = new ProductScan(s, new TableScan(fm, rightTf));
                s = new SelectScan(s, toPredicate(j.on));
            }
        }

        // WHERE（1表かつ等値 かつ 登録済みハッシュ索引があれば、IndexSelectScan に置換）
        if (ast.joins.isEmpty() && idxReg != null) {
            for (Ast.Predicate p : ast.where) {
                if (p.left instanceof Ast.Expr.Col col && p.right instanceof Ast.Expr.I val) {
                    String colName = col.name.contains(".") ? col.name.substring(col.name.indexOf('.') + 1) : col.name;
                    Optional<app.index.HashIndex> oidx = idxReg.findHashIndex(ast.from.table, colName);
                    if (oidx.isPresent()) {
                        s = new IndexSelectScan(fm, baseLayout, ast.from.table + ".tbl", oidx.get(), val.v);
                    } else {
                        s = new SelectScan(s, toPredicate(p));
                    }
                } else {
                    s = new SelectScan(s, toPredicate(p));
                }
            }
            System.out.println("[PLAN] using index on students.id");
        } else {
            for (Ast.Predicate p : ast.where)
                s = new SelectScan(s, toPredicate(p));
            System.out.println("[PLAN] using full table scan");
        }

        // PROJECTION
        if (!ast.projections.isEmpty()) {
            s = new ProjectScan(s, ast.projections);
        }

        // ORDER BY（単一列）
        if (ast.orderBy != null) {
            String fld = ast.orderBy.field.contains(".")
                    ? ast.orderBy.field.substring(ast.orderBy.field.indexOf('.') + 1)
                    : ast.orderBy.field;
            s = new OrderByScan(s, fld, ast.orderBy.asc);
        }

        // LIMIT
        if (ast.limit != null) {
            s = new LimitScan(s, ast.limit);
        }

        return s;
    }

    private Predicate toPredicate(Ast.Predicate p) {
        if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.Col) {
            return Predicate.eqField(((Ast.Expr.Col) p.left).name, ((Ast.Expr.Col) p.right).name);
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.I) {
            return Predicate.eqInt(((Ast.Expr.Col) p.left).name, ((Ast.Expr.I) p.right).v);
        } else if (p.left instanceof Ast.Expr.Col && p.right instanceof Ast.Expr.S) {
            return Predicate.eqString(((Ast.Expr.Col) p.left).name, ((Ast.Expr.S) p.right).v);
        }
        throw new IllegalArgumentException("unsupported predicate");
    }
}
