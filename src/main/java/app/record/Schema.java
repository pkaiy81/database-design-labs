package app.record;

import java.util.LinkedHashMap;
import java.util.Map;

public final class Schema {
    // フィールド定義（追加順を保持）
    private final Map<String, FieldDef> fields = new LinkedHashMap<>();

    public Schema addInt(String name) {
        fields.put(name, new FieldDef(FieldType.INT, 0));
        return this;
    }

    /** 文字列は最大長（文字数）を指定。バイト長はUTF-8で長さ分＋長さヘッダ4B */
    public Schema addString(String name, int maxChars) {
        fields.put(name, new FieldDef(FieldType.STRING, maxChars));
        return this;
    }

    public Map<String, FieldDef> fields() {
        return fields;
    }

    public static final class FieldDef {
        public final FieldType type;
        final int maxChars;

        FieldDef(FieldType t, int m) {
            this.type = t;
            this.maxChars = m;
        }
    }
}
