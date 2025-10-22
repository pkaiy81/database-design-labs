package app.sql;

import java.util.*;

public final class Ast {
    public sealed interface Statement permits SelectStmt, InsertStmt, UpdateStmt, DeleteStmt, ExplainStmt {
    }

    public static abstract class SelectItem {
        public static final class Column extends SelectItem {
            public final String name;

            public Column(String n) {
                this.name = n;
            }
        }

        public static final class Agg extends SelectItem {
            public final String func;
            public final String arg;

            public Agg(String f, String a) {
                this.func = f;
                this.arg = a;
            }
        }
    }

    public static final class Having {
        public final String func; // COUNT/SUM/AVG/MIN/MAX
        public final String arg; // 列名 or null (COUNT(*))
        public final String op; // ">", ">=", "<", "<=", "="
        public final int rhs; // 右辺 int

        public Having(String func, String arg, String op, int rhs) {
            this.func = func;
            this.arg = arg;
            this.op = op;
            this.rhs = rhs;
        }
    }

    public static final class CreateIndexStmt {
        public final String indexName;
        public final String tableName;
        public final String columnName;

        public CreateIndexStmt(String in, String tn, String cn) {
            this.indexName = in;
            this.tableName = tn;
            this.columnName = cn;
        }
    }

    public static final class SelectStmt implements Statement {
        public final boolean distinct;
        public final List<SelectItem> projections;
        public final From from;
        public final List<Join> joins;
        public final List<Predicate> where;
        public final String groupBy; // 単一列（null可）
        public final Having having; // null 可
        public final OrderBy orderBy; // null 可
        public final Integer limit; // null 可

        public SelectStmt(boolean distinct, List<SelectItem> projections, From from, List<Join> joins,
                List<Predicate> where, String groupBy, Having having, OrderBy orderBy, Integer limit) {
            this.distinct = distinct;
            this.projections = projections;
            this.from = from;
            this.joins = joins;
            this.where = where;
            this.groupBy = groupBy;
            this.having = having;
            this.orderBy = orderBy;
            this.limit = limit;
        }
    }

    public static final class InsertStmt implements Statement {
        public final String table;
        public final List<String> columns;
        public final List<Expr> values;

        public InsertStmt(String table, List<String> columns, List<Expr> values) {
            this.table = table;
            this.columns = List.copyOf(columns);
            this.values = List.copyOf(values);
        }
    }

    public static final class ExplainStmt implements Statement {
        public final SelectStmt select;

        public ExplainStmt(SelectStmt select) {
            this.select = Objects.requireNonNull(select);
        }
    }

    public static final class UpdateStmt implements Statement {
        public final String table;
        public final List<Assignment> assignments;
        public final List<Predicate> where;

        public UpdateStmt(String table, List<Assignment> assignments, List<Predicate> where) {
            this.table = table;
            this.assignments = List.copyOf(assignments);
            this.where = List.copyOf(where);
        }

        public static final class Assignment {
            public final String column;
            public final Expr value;

            public Assignment(String column, Expr value) {
                this.column = column;
                this.value = value;
            }
        }
    }

    public static final class DeleteStmt implements Statement {
        public final String table;
        public final List<Predicate> where;

        public DeleteStmt(String table, List<Predicate> where) {
            this.table = table;
            this.where = List.copyOf(where);
        }
    }

    public static final class From {
        public final String table;

        public From(String t) {
            this.table = t;
        }
    }

    public static final class Join {
        public final String table;
        public final Predicate on;

        public Join(String t, Predicate p) {
            this.table = t;
            this.on = p;
        }
    }

    public static final class OrderBy {
        public final String field;
        public final boolean asc;

        public OrderBy(String f, boolean a) {
            this.field = f;
            this.asc = a;
        }
    }

    public static class Predicate {
        public final Expr left;
        public final Expr right;

        public Predicate(Expr l, Expr r) {
            this.left = l;
            this.right = r;
        }
    }

    // PredicateBetween
    // (string, int, int)
    public static final class PredicateBetween extends Predicate {
        public final int low;
        public final int high;

        public PredicateBetween(Expr left, int low, int high) {
            super(left, null);
            this.low = low;
            this.high = high;
        }
    }

    // PredicateCompare
    public static final class PredicateCompare extends Predicate {
        public final String op; // "=", ">", "<", ">=", "<="

        public PredicateCompare(String col, CompareOp le, int right) {
            super(new Expr.Col(col), new Expr.I(right));
            this.op = le.op;
        }
    }

    public static abstract class Expr {
        public static final class Col extends Expr {
            public final String name;

            public Col(String n) {
                this.name = n;
            }
        }

        public static final class S extends Expr {
            public final String v;

            public S(String v) {
                this.v = v;
            }
        }

        public static final class I extends Expr {
            public final int v;

            public I(int v) {
                this.v = v;
            }
        }
    }

    // CompareOp
    // "=", ">", "<", ">=", "<="
    // LE, GE, EQ, LT, GT
    public static final class CompareOp {
        public static final CompareOp LE = new CompareOp("<=");
        public static final CompareOp GE = new CompareOp(">=");
        public static final CompareOp EQ = new CompareOp("=");
        public static final CompareOp LT = new CompareOp("<");
        public static final CompareOp GT = new CompareOp(">");
        public final String op;

        public CompareOp(String o) {
            this.op = o;
        }
    }

}
