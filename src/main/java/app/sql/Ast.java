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
            public final String func; // COUNT/SUM/AVG/MIN/MAX
            public final String arg; // COUNT(*) の場合 null

            public Agg(String func, String arg) {
                this.func = func;
                this.arg = arg;
            }
        }
    }

    public static final class SelectStmt {
        public final List<SelectItem> projections;
        public final From from;
        public final List<Join> joins;
        public final List<Predicate> where;
        public final OrderBy orderBy;
        public final Integer limit;
        public final String groupBy;

        public SelectStmt(List<SelectItem> projections, From from, List<Join> joins,
                List<Predicate> where, OrderBy orderBy, Integer limit, String groupBy) {
            this.projections = projections;
            this.from = from;
            this.joins = joins;
            this.where = where;
            this.orderBy = orderBy;
            this.limit = limit;
            this.groupBy = groupBy;
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
