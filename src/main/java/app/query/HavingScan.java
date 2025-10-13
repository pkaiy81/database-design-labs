package app.query;

import java.util.Objects;

/** GroupBy の結果（集約列名）に対し、単一条件でフィルタする最小版 HAVING */
public final class HavingScan implements Scan {
    public enum Op {
        GT, GE, LT, LE, EQ
    }

    private final Scan child;
    private final String field; // 集約結果列名（count, sum_x, ...）
    private final Op op;
    private final int rhs; // 右辺は int 前提

    public HavingScan(Scan child, String field, Op op, int rhs) {
        this.child = Objects.requireNonNull(child);
        this.field = Objects.requireNonNull(field);
        this.op = Objects.requireNonNull(op);
        this.rhs = rhs;
    }

    @Override
    public void beforeFirst() {
        child.beforeFirst();
    }

    @Override
    public boolean next() {
        while (child.next()) {
            int lhs = readAsInt(child, field);
            if (switch (op) {
                case GT -> lhs > rhs;
                case GE -> lhs >= rhs;
                case LT -> lhs < rhs;
                case LE -> lhs <= rhs;
                case EQ -> lhs == rhs;
            })
                return true;
        }
        return false;
    }

    private static int readAsInt(Scan s, String f) {
        try {
            return s.getInt(f);
        } catch (Exception e) {
            try {
                String v = s.getString(f);
                if (v != null && v.matches("-?\\d+"))
                    return Integer.parseInt(v);
            } catch (Exception ignore) {
            }
        }
        throw new IllegalArgumentException("HAVING field not numeric: " + f);
    }

    @Override
    public int getInt(String field) {
        return child.getInt(field);
    }

    @Override
    public String getString(String field) {
        return child.getString(field);
    }

    @Override
    public void close() {
        child.close();
    }
}
