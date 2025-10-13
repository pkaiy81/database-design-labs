package app.sql;

import java.util.ArrayList;
import java.util.List;

import static app.sql.TokenType.*;

public final class Parser {
    private final Lexer lx;

    public Parser(String sql) {
        this.lx = new Lexer(sql);
    }

    public Ast.SelectStmt parseSelect() {
        expect(SELECT);
        List<String> proj = parseProjections();
        expect(FROM);
        String base = parseIdentQualified();
        Ast.From from = new Ast.From(base);

        List<Ast.Join> joins = new ArrayList<>();
        while (lx.type() == JOIN) {
            lx.next();
            String jt = parseIdentQualified();
            expect(ON);
            Ast.Predicate on = parseEqPredicate();
            joins.add(new Ast.Join(jt, on));
        }

        List<Ast.Predicate> where = new ArrayList<>();
        if (lx.type() == WHERE) {
            lx.next();
            where.add(parseEqPredicate());
            while (lx.type() == AND) {
                lx.next();
                where.add(parseEqPredicate());
            }
        }

        Ast.OrderBy ob = null;
        if (lx.type() == ORDER) {
            lx.next();
            expect(BY);
            String fld = parseIdentQualified();
            boolean asc = true;
            if (lx.type() == ASC) {
                lx.next();
                asc = true;
            } else if (lx.type() == DESC) {
                lx.next();
                asc = false;
            }
            ob = new Ast.OrderBy(fld, asc);
        }

        Integer limit = null;
        if (lx.type() == LIMIT) {
            lx.next();
            if (lx.type() != TokenType.INT)
                throw err("LIMIT requires integer");
            limit = Integer.parseInt(lx.text());
            lx.next();
        }

        expect(EOF);
        return new Ast.SelectStmt(proj, from, joins, where, ob, limit);
    }

    private List<String> parseProjections() {
        List<String> list = new ArrayList<>();
        if (lx.type() == STAR) {
            lx.next();
            return list;
        } // empty means *
        list.add(parseIdentQualified());
        while (lx.type() == COMMA) {
            lx.next();
            list.add(parseIdentQualified());
        }
        return list;
    }

    private Ast.Predicate parseEqPredicate() {
        Ast.Expr left = parseExpr();
        expect(EQ);
        Ast.Expr right = parseExpr();
        return new Ast.Predicate(left, right);
    }

    private Ast.Expr parseExpr() {
        switch (lx.type()) {
            case STRING: {
                String v = lx.text();
                lx.next();
                return new Ast.Expr.S(v);
            }
            case INT: {
                int v = Integer.parseInt(lx.text());
                lx.next();
                return new Ast.Expr.I(v);
            }
            case IDENT: {
                String id = parseIdentQualified();
                return new Ast.Expr.Col(id);
            }
            default:
                throw err("expr");
        }
    }

    private String parseIdentQualified() {
        if (lx.type() != IDENT)
            throw err("identifier");
        String a = lx.text();
        lx.next();
        if (lx.type() == DOT) {
            lx.next();
            if (lx.type() != IDENT)
                throw err("identifier");
            String b = lx.text();
            lx.next();
            return a + "." + b;
        }
        return a;
    }

    private void expect(TokenType t) {
        if (lx.type() != t)
            throw err("expected " + t + " but got " + lx.type());
        lx.next();
    }

    private RuntimeException err(String m) {
        return new RuntimeException("Parse error: " + m);
    }
}
