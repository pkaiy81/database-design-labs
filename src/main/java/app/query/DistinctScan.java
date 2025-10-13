package app.query;

import java.util.*;

/** 子Scanの行を、指定フィールドの組で一意化して返す */
public final class DistinctScan implements Scan {
    private final Scan child;
    private final List<String> fields;

    private static final class Row {
        final Map<String, Object> vals = new HashMap<>();
    }

    private final List<Row> rows = new ArrayList<>();
    private int pos = -1;

    public DistinctScan(Scan child, List<String> fields) {
        this.child = Objects.requireNonNull(child);
        this.fields = List.copyOf(fields);
    }

    @Override
    public void beforeFirst() {
        rows.clear();
        child.beforeFirst();
        Set<List<Object>> seen = new HashSet<>();
        while (child.next()) {
            List<Object> key = new ArrayList<>(fields.size());
            Row r = new Row();
            for (String f : fields) {
                Object v = readSmart(child, f);
                key.add(v);
                r.vals.put(f, v);
            }
            if (seen.add(key))
                rows.add(r);
        }
        pos = -1;
    }

    private Object readSmart(Scan s, String f) {
        // 文字列優先 → ダメなら int
        try {
            String sv = s.getString(f);
            // 空文字も正規の値として扱う（必要なら isBlank() で除外可）
            return sv;
        } catch (Exception ignore) {
        }
        try {
            return Integer.valueOf(s.getInt(f));
        } catch (Exception ignore) {
        }
        return null;
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
        if (v instanceof String && ((String) v).matches("-?\\d+"))
            return Integer.parseInt((String) v);
        throw new IllegalArgumentException("not an int: " + field);
    }

    @Override
    public String getString(String field) {
        Object v = rows.get(pos).vals.get(field);
        if (v != null)
            return v.toString();
        throw new IllegalArgumentException("no such field: " + field);
    }

    @Override
    public void close() {
        child.close();
    }
}
