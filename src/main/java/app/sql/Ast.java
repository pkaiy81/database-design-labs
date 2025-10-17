package app.sql;

import java.util.*;

public final class Ast {
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

    public static final class SelectStmt {
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

    public static final class Predicate {
        public final Expr left;
        public final Expr right;

        public Predicate(Expr l, Expr r) {
            this.left = l;
            this.right = r;
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
}
