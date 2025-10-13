package app.sql;

import app.metadata.MetadataManager;
import app.query.*;
import app.record.*;
import app.storage.FileMgr;

import java.util.List;

public final class Planner {
    private final FileMgr fm;
    private final MetadataManager mdm;

    public Planner(FileMgr fm, MetadataManager mdm) {
        this.fm = fm;
        this.mdm = mdm;
    }

    public Scan plan(String sql) {
        Ast.SelectStmt ast = new Parser(sql).parseSelect();

        // FROM
        Layout baseLayout = mdm.getLayout(ast.from.table);
        TableFile baseTf = new TableFile(fm, ast.from.table + ".tbl", baseLayout);
        Scan s = new TableScan(fm, baseTf);

        // JOIN … 直積＋等値条件
        for (Ast.Join j : ast.joins) {
            Layout jl = mdm.getLayout(j.table);
            TableFile jtf = new TableFile(fm, j.table + ".tbl", jl);
            s = new ProductScan(s, new TableScan(fm, jtf));
            s = new SelectScan(s, toPredicate(j.on));
        }

        // WHERE（AND連結）
        for (Ast.Predicate p : ast.where)
            s = new SelectScan(s, toPredicate(p));

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
