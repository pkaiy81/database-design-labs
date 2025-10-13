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
        final Map<String, Object> vals = new HashMap<>();
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

    private Object readSmart(Scan s, String f) {
        String sv = null;
        boolean sOk = false;
        Integer iv = null;
        boolean iOk = false;
        try {
            sv = s.getString(f);
            sOk = true;
        } catch (Exception ignore) {
        }
        try {
            iv = s.getInt(f);
            iOk = true;
        } catch (Exception ignore) {
        }

        if (iOk && !sOk)
            return iv;
        if (sOk && !iOk)
            return sv;
        if (iOk && sOk) {
            // 両方成功：sv が純粋な数字で iv と一致なら int を優先。それ以外は文字列優先。
            if (sv != null && sv.matches("-?\\d+") && (iv != null) && sv.equals(Integer.toString(iv))) {
                return iv;
            } else {
                return sv;
            }
        }
        return null;
    }

    @Override
    public void beforeFirst() {
        rows.clear();
        child.beforeFirst();
        while (child.next()) {
            Row r = new Row();
            // 並べ替えキー
            r.vals.put(orderField, readSmart(child, orderField));
            // それ以外の列
            for (String f : carryFields) {
                if (f.equals(orderField))
                    continue;
                r.vals.put(f, readSmart(child, f));
            }
            rows.add(r);
        }
        rows.sort((a, b) -> {
            Object va = a.vals.get(orderField);
            Object vb = b.vals.get(orderField);
            int cmp;
            if (va instanceof Integer && vb instanceof Integer)
                cmp = Integer.compare((Integer) va, (Integer) vb);
            else
                cmp = java.util.Objects.toString(va, "").compareTo(java.util.Objects.toString(vb, ""));
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
        Object v = rows.get(pos).vals.get(field);
        if (v instanceof Integer)
            return (Integer) v;
        if (v instanceof String) {
            String sv = (String) v;
            if (sv.matches("-?\\d+"))
                return Integer.parseInt(sv);
        }
        throw new IllegalArgumentException("not an int or not materialized: " + field);
    }

    @Override
    public String getString(String field) {
        Object v = rows.get(pos).vals.get(field);
        if (v != null)
            return v.toString();
        throw new IllegalArgumentException("not materialized: " + field);
    }

    @Override
    public void close() {
        child.close();
    }
}
