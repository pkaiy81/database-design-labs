// src/main/java/app/query/OrderByScan.java
package app.query;

import java.util.*;

/** 全行を一旦メモリに取り込み、単一キーで昇順/降順ソートして返す */
public final class OrderByScan implements Scan {
    private final Scan child;
    private final String orderField;
    private final boolean asc;

    private static final class Row {
        final Map<String, Object> vals = new HashMap<>();
    }

    private List<Row> rows = new ArrayList<>();
    private int pos = -1;

    public OrderByScan(Scan child, String orderField, boolean asc) {
        this.child = Objects.requireNonNull(child);
        this.orderField = Objects.requireNonNull(orderField);
        this.asc = asc;
    }

    @Override
    public void beforeFirst() {
        rows.clear();
        child.beforeFirst();
        while (child.next()) {
            Row r = new Row();
            // ★ 先に String を試す → ダメなら Int
            Object orderVal;
            try {
                orderVal = child.getString(orderField);
            } catch (Exception e) {
                orderVal = child.getInt(orderField);
            }
            r.vals.put(orderField, orderVal);
            rows.add(r);
        }
        rows.sort((a, b) -> {
            Object va = a.vals.get(orderField);
            Object vb = b.vals.get(orderField);
            int cmp;
            if (va instanceof Integer && vb instanceof Integer) {
                cmp = Integer.compare((Integer) va, (Integer) vb);
            } else {
                cmp = va.toString().compareTo(vb.toString());
            }
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
        throw new IllegalArgumentException("field not materialized as int: " + field);
    }

    @Override
    public String getString(String field) {
        Object v = rows.get(pos).vals.get(field);
        if (v != null)
            return v.toString();
        throw new IllegalArgumentException("field not materialized as string: " + field);
    }

    @Override
    public void close() {
        child.close();
    }
}
