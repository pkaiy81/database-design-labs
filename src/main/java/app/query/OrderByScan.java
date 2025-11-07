package app.query;

import java.util.*;

/**
 * 全行をメモリに取り込み、単一キーで昇順/降順ソート。
 * 並べ替えキーに加えて carryFields で指定された列もマテリアライズし、
 * 並べ替え後の getInt/getString で取得できるようにする。
 */
public final class OrderByScan implements Scan {
    private final Scan child;
    private final String orderField;
    private final boolean asc;
    private final List<String> carryFields; // 並べ替え後も参照される列

    private static final class Row {
        final Map<String, FieldValue> vals = new HashMap<>();
    }

    private static final class FieldValue {
        final Integer intValue;
        final String stringValue;

        FieldValue(Integer intValue, String stringValue) {
            this.intValue = intValue;
            this.stringValue = stringValue;
        }

        Integer asInt() {
            if (intValue != null)
                return intValue;
            if (stringValue != null && stringValue.matches("-?\\d+"))
                return Integer.parseInt(stringValue);
            throw new IllegalArgumentException("not an int value");
        }

        String asString() {
            if (stringValue != null)
                return stringValue;
            if (intValue != null)
                return Integer.toString(intValue);
            return null;
        }

        int compareTo(FieldValue other) {
            if (this == other)
                return 0;
            if (other == null)
                return (this.intValue == null && this.stringValue == null) ? 0 : 1;
            if (this.intValue != null && other.intValue != null)
                return Integer.compare(this.intValue, other.intValue);
            String a = java.util.Objects.toString(this.asString(), "");
            String b = java.util.Objects.toString(other.asString(), "");
            return a.compareTo(b);
        }
    }

    private List<Row> rows = new ArrayList<>();
    private int pos = -1;

    public OrderByScan(Scan child, String orderField, boolean asc, Collection<String> carryFields) {
        this.child = Objects.requireNonNull(child);
        this.orderField = Objects.requireNonNull(orderField);
        this.asc = asc;
        // 重複排除＋順序維持
        LinkedHashSet<String> set = new LinkedHashSet<>(carryFields == null ? List.of() : carryFields);
        set.add(orderField); // 並べ替えキーは必ず保持
        this.carryFields = new ArrayList<>(set);
    }

    private FieldValue captureField(Scan s, String f) {
        Integer iv = null;
        try {
            iv = s.getInt(f);
        } catch (Exception ignore) {
        }
        String sv = null;
        try {
            sv = s.getString(f);
        } catch (Exception ignore) {
        }
        if (iv == null && sv == null)
            return new FieldValue(null, null);
        return new FieldValue(iv, sv);
    }

    @Override
    public void beforeFirst() {
        rows.clear();
        child.beforeFirst();
        while (child.next()) {
            Row r = new Row();
            // 並べ替えキー
            r.vals.put(orderField, captureField(child, orderField));
            // それ以外の列
            for (String f : carryFields) {
                if (f.equals(orderField))
                    continue;
                r.vals.put(f, captureField(child, f));
            }
            rows.add(r);
        }
        rows.sort((a, b) -> {
            FieldValue va = a.vals.get(orderField);
            FieldValue vb = b.vals.get(orderField);
            int cmp = (va != null) ? va.compareTo(vb) : (vb == null ? 0 : -1);
            return asc ? cmp : -cmp;
        });
        pos = -1;
    }

    @Override
    public boolean next() {
        return ++pos < rows.size();
    }

    @Override
    public int getInt(String field) {
        FieldValue v = rows.get(pos).vals.get(field);
        if (v == null)
            throw new IllegalArgumentException("not materialized: " + field);
        return v.asInt();
    }

    @Override
    public String getString(String field) {
        FieldValue v = rows.get(pos).vals.get(field);
        if (v != null) {
            String s = v.asString();
            if (s != null)
                return s;
        }
        throw new IllegalArgumentException("not materialized: " + field);
    }

    @Override
    public void close() {
        child.close();
    }
}
