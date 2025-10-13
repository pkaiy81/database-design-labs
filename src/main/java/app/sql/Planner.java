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
            Layout jl = mdm.getLayout(j.table);
            TableFile jtf = new TableFile(fm, j.table + ".tbl", jl);
            s = new ProductScan(s, new TableScan(fm, jtf));
            s = new SelectScan(s, toPredicate(j.on));
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
