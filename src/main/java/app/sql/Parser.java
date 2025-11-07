package app.sql;

import java.util.ArrayList;
import java.util.List;

import static app.sql.TokenType.*;

public final class Parser {
    private final Lexer lx;

    public Parser(String sql) {
        this.lx = new Lexer(sql);
    }

    public Ast.Statement parseStatement() {
        return switch (lx.type()) {
            case SELECT -> parseSelect();
            case EXPLAIN -> parseExplain();
            case CREATE -> parseCreate();
            case DROP -> parseDrop();
            case INSERT -> parseInsert();
            case UPDATE -> parseUpdate();
            case DELETE -> parseDelete();
            default -> throw err("unsupported statement start: " + lx.type());
        };
    }

    private Ast.Statement parseCreate() {
        expect(CREATE);
        return switch (lx.type()) {
            case TABLE -> parseCreateTable();
            case INDEX -> parseCreateIndex();
            default -> throw err("unsupported CREATE target: " + lx.type());
        };
    }

    private Ast.Statement parseDrop() {
        expect(DROP);
        return switch (lx.type()) {
            case TABLE -> parseDropTable();
            case INDEX -> parseDropIndex();
            default -> throw err("unsupported DROP target: " + lx.type());
        };
    }

    private Ast.ExplainStmt parseExplain() {
        expect(EXPLAIN);
        Ast.SelectStmt select = parseSelect();
        return new Ast.ExplainStmt(select);
    }

    public Ast.CreateIndexStmt parseCreateIndex() {
        // CREATE already consumed by parseCreate()
        expect(INDEX);

        String idx = parseIdentQualified(); // index 名（スキーマ付きでも可）

        expect(TokenType.ON);

        // テーブル名はまず単体 IDENT として読む
        String tbl = parseIdent(); // parseIdentQualified() ではなく、単体 IDENT を読む想定

        String col;
        if (lx.type() == TokenType.LPAREN) {
            // CREATE INDEX idx ON t(c)
            lx.next();
            col = parseIdent();
            expect(TokenType.RPAREN);
        } else if (lx.type() == TokenType.DOT) {
            // CREATE INDEX idx ON t.c
            lx.next(); // consume '.'
            col = parseIdent(); // 単体IDENTで列名を読む
        } else {
            throw new ParseException("expected '(' or '.' after table name");
        }

        // 任意: USING BTREE を食べる（TokenType に USING/BTREE を追加済み前提）
        if (lx.type() == TokenType.USING) {
            lx.next();
            expect(TokenType.BTREE);
        }

        expect(TokenType.EOF);
        return new Ast.CreateIndexStmt(idx, tbl, col);
    }

    private Ast.DropIndexStmt parseDropIndex() {
        expect(INDEX);
        String name = parseIdentQualified();
        expect(EOF);
        return new Ast.DropIndexStmt(name);
    }

    private Ast.CreateTableStmt parseCreateTable() {
        expect(TABLE);
        String tableName = parseIdentQualified();
        expect(LPAREN);
        List<Ast.CreateTableStmt.ColumnDef> columns = new ArrayList<>();
        columns.add(parseCreateTableColumn());
        while (lx.type() == COMMA) {
            lx.next();
            columns.add(parseCreateTableColumn());
        }
        expect(RPAREN);
        expect(EOF);
        return new Ast.CreateTableStmt(tableName, columns);
    }

    private Ast.DropTableStmt parseDropTable() {
        expect(TABLE);
        String tableName = parseIdentQualified();
        expect(EOF);
        return new Ast.DropTableStmt(tableName);
    }

    private Ast.CreateTableStmt.ColumnDef parseCreateTableColumn() {
        String name = parseIdentQualified();
        Ast.CreateTableStmt.ColumnType type;
        Integer len = null;
        if (lx.type() == IDENT && lx.text().equalsIgnoreCase("INT")) {
            type = Ast.CreateTableStmt.ColumnType.INT;
            lx.next();
        } else if (lx.type() == IDENT && lx.text().equalsIgnoreCase("STRING")) {
            lx.next();
            type = Ast.CreateTableStmt.ColumnType.STRING;
            expect(LPAREN);
            len = parseIntLiteral();
            expect(RPAREN);
        } else {
            throw err("column type (INT or STRING(n))");
        }
        return new Ast.CreateTableStmt.ColumnDef(name, type, len);
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

    private Ast.InsertStmt parseInsert() {
        expect(INSERT);
        expect(INTO);
        String table = parseIdentQualified();
        expect(LPAREN);
        List<String> columns = parseIdentifierList();
        expect(RPAREN);
        expect(VALUES);
        expect(LPAREN);
        List<Ast.Expr> values = parseLiteralList();
        expect(RPAREN);
        if (columns.size() != values.size())
            throw err("columns and values count mismatch");
        expect(EOF);
        return new Ast.InsertStmt(table, columns, values);
    }

    private Ast.UpdateStmt parseUpdate() {
        expect(UPDATE);
        String table = parseIdentQualified();
        expect(SET);
        List<Ast.UpdateStmt.Assignment> assignments = new ArrayList<>();
        assignments.add(parseAssignment());
        while (lx.type() == COMMA) {
            lx.next();
            assignments.add(parseAssignment());
        }
        List<Ast.Predicate> where = parseWhereClauseIfPresent();
        expect(EOF);
        return new Ast.UpdateStmt(table, assignments, where);
    }

    private Ast.DeleteStmt parseDelete() {
        expect(DELETE);
        expect(FROM);
        String table = parseIdentQualified();
        List<Ast.Predicate> where = parseWhereClauseIfPresent();
        expect(EOF);
        return new Ast.DeleteStmt(table, where);
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

    // parseIdentifier()
    private String parseIdentifier() {
        if (lx.type() != IDENT)
            throw err("identifier");
        String id = lx.text();
        lx.next();
        return id;
    }

    // matchSymbol()/matchKeyword()
    private boolean matchSymbol(String sym) {
        TokenType t = lx.type();
        if (t == TokenType.SYMBOL && lx.text().equals(sym)) {
            lx.next();
            return true;
        }
        switch (sym) {
            case "<=":
                if (t == TokenType.LE) {
                    lx.next();
                    return true;
                }
                break;
            case ">=":
                if (t == TokenType.GE) {
                    lx.next();
                    return true;
                }
                break;
            case "<":
                if (t == TokenType.LT) {
                    lx.next();
                    return true;
                }
                break;
            case ">":
                if (t == TokenType.GT) {
                    lx.next();
                    return true;
                }
                break;
            case "=":
                if (t == TokenType.EQ) {
                    lx.next();
                    return true;
                }
                break;
            default:
                break;
        }
        return false;
    }

    private boolean matchKeyword(String kw) {
        TokenType t = lx.type();
        if (t == TokenType.KEYWORD && lx.text().equals(kw)) {
            lx.next();
            return true;
        }
        if (t.name().equalsIgnoreCase(kw)) {
            lx.next();
            return true;
        }
        return false;
    }

    // parseIntLiteral()
    private int parseIntLiteral() {
        if (lx.type() != INT)
            throw err("integer literal");
        int v = Integer.parseInt(lx.text());
        lx.next();
        return v;
    }

    // expectKeyword()
    private void expectKeyword(String kw) {
        TokenType t = lx.type();
        if (t == TokenType.KEYWORD && lx.text().equals(kw)) {
            lx.next();
            return;
        }
        if (t.name().equalsIgnoreCase(kw)) {
            lx.next();
            return;
        }
        throw err("expected keyword " + kw);
    }

    private Ast.Predicate parseEqPredicate() {
        String col = parseIdentifier();

        if (matchKeyword("BETWEEN")) {
            int lo = parseIntLiteral();
            expectKeyword("AND");
            int hi = parseIntLiteral();
            return new Ast.PredicateBetween(new Ast.Expr.Col(col), lo, hi);
        }
        if (matchSymbol("<="))
            return new Ast.PredicateCompare(col, Ast.CompareOp.LE, parseIntLiteral());
        if (matchSymbol(">="))
            return new Ast.PredicateCompare(col, Ast.CompareOp.GE, parseIntLiteral());
        if (matchSymbol("<"))
            return new Ast.PredicateCompare(col, Ast.CompareOp.LT, parseIntLiteral());
        if (matchSymbol(">"))
            return new Ast.PredicateCompare(col, Ast.CompareOp.GT, parseIntLiteral());
        if (matchSymbol("="))
            return new Ast.PredicateCompare(col, Ast.CompareOp.EQ, parseIntLiteral());

        throw err("expected comparison operator or BETWEEN");
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

    private String parseIdent() {
        if (lx.type() != IDENT)
            throw err("identifier");
        String a = lx.text();
        lx.next();
        return a;
    }

    private void expect(TokenType t) {
        if (lx.type() != t)
            throw err("expected " + t + " but got " + lx.type());
        lx.next();
    }

    private List<String> parseIdentifierList() {
        List<String> cols = new ArrayList<>();
        cols.add(parseIdentQualified());
        while (lx.type() == COMMA) {
            lx.next();
            cols.add(parseIdentQualified());
        }
        return cols;
    }

    private List<Ast.Expr> parseLiteralList() {
        List<Ast.Expr> values = new ArrayList<>();
        values.add(parseLiteral());
        while (lx.type() == COMMA) {
            lx.next();
            values.add(parseLiteral());
        }
        return values;
    }

    private Ast.Expr parseLiteral() {
        if (lx.type() == INT) {
            int v = Integer.parseInt(lx.text());
            lx.next();
            return new Ast.Expr.I(v);
        }
        if (lx.type() == STRING) {
            String v = lx.text();
            lx.next();
            return new Ast.Expr.S(v);
        }
        throw err("literal value");
    }

    private Ast.UpdateStmt.Assignment parseAssignment() {
        String column = parseIdentQualified();
        expect(EQ);
        Ast.Expr value = parseLiteral();
        return new Ast.UpdateStmt.Assignment(column, value);
    }

    private List<Ast.Predicate> parseWhereClauseIfPresent() {
        List<Ast.Predicate> where = new ArrayList<>();
        if (lx.type() == WHERE) {
            lx.next();
            where.add(parseEqPredicate());
            while (lx.type() == AND) {
                lx.next();
                where.add(parseEqPredicate());
            }
        }
        return where;
    }

    private RuntimeException err(String m) {
        return new RuntimeException("Parse error: " + m);
    }
}
