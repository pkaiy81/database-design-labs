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

        boolean distinct = false;
        if (lx.type() == DISTINCT) {
            lx.next();
            distinct = true;
        }

        List<Ast.SelectItem> proj = parseProjectionItems();
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

        String groupBy = null;
        if (lx.type() == GROUP) {
            lx.next();
            expect(BY);
            groupBy = parseIdentQualified();
        }

        Ast.Having having = null;
        if (lx.type() == HAVING) {
            lx.next();
            having = parseHaving();
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
            if (lx.type() != INT)
                throw err("LIMIT requires integer");
            limit = Integer.parseInt(lx.text());
            lx.next();
        }

        expect(EOF);
        return new Ast.SelectStmt(distinct, proj, from, joins, where, groupBy, having, ob, limit);
    }

    private Ast.Having parseHaving() {
        // HAVING <AGG>(<col|*>) <op> <int>
        String func = null, arg = null, opStr;
        switch (lx.type()) {
            case COUNT:
            case SUM:
            case AVG:
            case MIN:
            case MAX:
                func = lx.type().name();
                lx.next();
                break;
            default:
                throw err("HAVING requires aggregate function");
        }
        expect(LPAREN);
        if (lx.type() == STAR) {
            lx.next();
            arg = null;
        } else {
            arg = parseIdentQualified();
        }
        expect(RPAREN);

        // op
        if (lx.type() == GT || lx.type() == GE || lx.type() == LT || lx.type() == LE || lx.type() == EQ) {
            opStr = lx.text();
            lx.next();
        } else
            throw err("HAVING operator");

        if (lx.type() != INT)
            throw err("HAVING rhs integer");
        int rhs = Integer.parseInt(lx.text());
        lx.next();

        return new Ast.Having(func, arg, opStr, rhs);
    }

    private List<Ast.SelectItem> parseProjectionItems() {
        List<Ast.SelectItem> list = new ArrayList<>();
        list.add(parseProjectionItem());
        while (lx.type() == COMMA) {
            lx.next();
            list.add(parseProjectionItem());
        }
        return list;
    }

    private Ast.SelectItem parseProjectionItem() {
        switch (lx.type()) {
            case STAR:
                lx.next();
                return new Ast.SelectItem.Column("*");
            case COUNT:
            case SUM:
            case AVG:
            case MIN:
            case MAX: {
                String func = lx.type().name();
                lx.next();
                expect(LPAREN);
                String arg = null;
                if (lx.type() == STAR) {
                    lx.next();
                    arg = null;
                } else {
                    arg = parseIdentQualified();
                }
                expect(RPAREN);
                return new Ast.SelectItem.Agg(func, arg);
            }
            case IDENT: {
                String col = parseIdentQualified();
                return new Ast.SelectItem.Column(col);
            }
            default:
                throw err("projection item");
        }
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
