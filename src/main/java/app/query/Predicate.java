package app.query;

import java.util.Objects;

public final class Predicate {
    public enum Op {
        EQ,
        LT,
        LE,
        GT,
        GE
    }

    private final String leftField;
    private final Op op;
    private final String rightField; // 右辺がフィールドの場合に使用
    private final Integer rightInt; // 右辺が定数(int)の場合に使用
    private final String rightStr; // 右辺が定数(string)の場合に使用

    private Predicate(String leftField, Op op, String rightField, Integer rightInt, String rightStr) {
        this.leftField = Objects.requireNonNull(leftField);
        this.op = Objects.requireNonNull(op);
        this.rightField = rightField;
        this.rightInt = rightInt;
        this.rightStr = rightStr;
    }

    public static Predicate eqField(String leftField, String rightField) {
        return new Predicate(leftField, Op.EQ, rightField, null, null);
    }

    public static Predicate eqInt(String leftField, int value) {
        return compareInt(leftField, Op.EQ, value);
    }

    public static Predicate eqString(String leftField, String value) {
        return new Predicate(leftField, Op.EQ, null, null, value);
    }

    public static Predicate compareInt(String leftField, Op op, int value) {
        if (op == null)
            throw new IllegalArgumentException("op must not be null");
        return new Predicate(leftField, op, null, value, null);
    }

    public boolean evaluate(Scan s) {
        switch (op) {
            case EQ:
                if (rightField != null) {
                    // 型は最低限の推定（例外は上位で拾う想定）。見つからない場合は文字列として比較トライ。
                    try {
                        return s.getInt(leftField) == s.getInt(rightField);
                    } catch (Exception ignore) {
                        return s.getString(leftField).equals(s.getString(rightField));
                    }
                } else if (rightInt != null) {
                    return s.getInt(leftField) == rightInt;
                } else {
                    return s.getString(leftField).equals(rightStr);
                }
            case LT:
                return s.getInt(leftField) < rightInt;
            case LE:
                return s.getInt(leftField) <= rightInt;
            case GT:
                return s.getInt(leftField) > rightInt;
            case GE:
                return s.getInt(leftField) >= rightInt;
        }
        throw new IllegalStateException("Unsupported op: " + op);
    }
}
