package app.query;

import java.util.*;

/**
 * 単一キーのハッシュ集約（キーなし=グローバル集約にも対応）
 * - groupField が null の場合は全体1グループ
 * - 集約列は INT を前提（MIN/MAX は String も対応）
 * - 出力列名:
 * - グループ列: groupField そのまま
 * - 集約列: 例えば COUNT(*) は "count", SUM(x) は "sum_x", AVG(x) は "avg_x" など
 */
public final class GroupByScan implements Scan {
    public static final class Spec {
        public final Agg agg;
        public final String field; // COUNT(*) の場合は null

        public Spec(Agg agg, String field) {
            this.agg = agg;
            this.field = field;
        }

        public String outName() {
            switch (agg) {
                case COUNT:
                    return "count";
                case SUM:
                    return "sum_" + field;
                case AVG:
                    return "avg_" + field;
                case MIN:
                    return "min_" + field;
                case MAX:
                    return "max_" + field;
            }
            throw new IllegalArgumentException();
        }
    }

    private final Scan child;
    private final String groupField; // null なら全体集約
    private final List<Spec> specs;

    private static final class Row {
        final Map<String, Object> vals = new HashMap<>();
    }

    private List<Row> results = new ArrayList<>();
    private int pos = -1;

    public GroupByScan(Scan child, String groupField, List<Spec> specs) {
        this.child = Objects.requireNonNull(child);
        this.groupField = groupField; // null 可
        this.specs = List.copyOf(specs);
    }

    @Override
    public void beforeFirst() {
        // 1) 走査してグループごとの集約値を計算
        Map<Object, Acc> map = new LinkedHashMap<>();
        child.beforeFirst();
        while (child.next()) {
            Object key;
            if (groupField == null) {
                key = "__global__";
            } else {
                // ★ INT優先で読む。失敗したらString。
                Object k = null;
                try {
                    k = Integer.valueOf(child.getInt(groupField));
                } catch (Exception ignore) {
                    try {
                        k = child.getString(groupField);
                    } catch (Exception ignore2) {
                        /* 最終的に null のまま */ }
                }
                key = (k == null ? "" : k);
            }
            map.computeIfAbsent(key, k -> new Acc()).accumulate(child, specs);
        }

        // 2) Acc -> Row へ変換
        results.clear();
        for (Map.Entry<Object, Acc> e : map.entrySet()) {
            Row r = new Row();
            if (groupField != null)
                r.vals.put(groupField, e.getKey());
            e.getValue().emitTo(r, groupField, specs);
            results.add(r);
        }
        pos = -1;
    }

    @Override
    public boolean next() {
        return ++pos < results.size();
    }

    @Override
    public int getInt(String field) {
        Object v = results.get(pos).vals.get(field);
        if (v instanceof Integer)
            return (Integer) v;
        if (v instanceof Long)
            return ((Long) v).intValue();
        throw new IllegalArgumentException("not an int: " + field);
    }

    @Override
    public String getString(String field) {
        Object v = results.get(pos).vals.get(field);
        if (v == null)
            throw new IllegalArgumentException("no such field: " + field);
        return v.toString();
    }

    @Override
    public void close() {
        child.close();
    }

    private static final class Acc {
        long count = 0;
        final Map<String, Long> sum = new HashMap<>();
        final Map<String, Integer> minInt = new HashMap<>();
        final Map<String, Integer> maxInt = new HashMap<>();
        final Map<String, String> minStr = new HashMap<>();
        final Map<String, String> maxStr = new HashMap<>();

        void accumulate(Scan s, List<Spec> specs) {
            count++;
            java.util.HashSet<String> addedSum = new java.util.HashSet<>();
            for (Spec sp : specs) {
                switch (sp.agg) {
                    case COUNT:
                        break;

                    case SUM:
                    case AVG: {
                        if (sp.field == null)
                            break;
                        if (addedSum.add(sp.field)) {
                            int v = s.getInt(sp.field);
                            sum.merge(sp.field, (long) v, Long::sum);
                        }
                        break;
                    }

                    case MIN: {
                        // ★ まずintを試す→ダメならstring
                        try {
                            int iv = s.getInt(sp.field);
                            minInt.merge(sp.field, iv, Math::min);
                        } catch (Exception ignore) {
                            String sv = s.getString(sp.field);
                            // 空文字は無視したいなら if (!sv.isBlank()) ...
                            minStr.merge(sp.field, sv, (a, b) -> (a.compareTo(b) <= 0) ? a : b);
                        }
                        break;
                    }

                    case MAX: {
                        // ★ まずintを試す→ダメならstring
                        try {
                            int iv = s.getInt(sp.field);
                            maxInt.merge(sp.field, iv, Math::max);
                        } catch (Exception ignore) {
                            String sv = s.getString(sp.field);
                            maxStr.merge(sp.field, sv, (a, b) -> (a.compareTo(b) >= 0) ? a : b);
                        }
                        break;
                    }
                }
            }
        }

        void emitTo(Row r, String groupField, List<Spec> specs) {
            for (Spec sp : specs) {
                switch (sp.agg) {
                    case COUNT:
                        r.vals.put(sp.outName(), (int) count);
                        break;
                    case SUM:
                        r.vals.put(sp.outName(), sum.getOrDefault(sp.field, 0L).intValue());
                        break;
                    case AVG: {
                        long s = sum.getOrDefault(sp.field, 0L);
                        r.vals.put(sp.outName(), (int) (count == 0 ? 0 : (s / count)));
                        break;
                    }
                    case MIN: {
                        if (minStr.containsKey(sp.field))
                            r.vals.put(sp.outName(), minStr.get(sp.field));
                        else
                            r.vals.put(sp.outName(), minInt.getOrDefault(sp.field, 0));
                        break;
                    }
                    case MAX: {
                        if (maxStr.containsKey(sp.field))
                            r.vals.put(sp.outName(), maxStr.get(sp.field));
                        else
                            r.vals.put(sp.outName(), maxInt.getOrDefault(sp.field, 0));
                        break;
                    }
                }
            }
        }
    }

    @FunctionalInterface
    private interface ThrowingSupplier<T> {
        T get() throws Exception;
    }

    @FunctionalInterface
    private interface ThrowingIntSupplier {
        int get() throws Exception;
    }
}
