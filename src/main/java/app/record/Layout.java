package app.record;

import java.util.LinkedHashMap;
import java.util.Map;

/** スキーマから各フィールドのオフセットとレコード全体サイズを決める */
public final class Layout {
    private final Schema schema;
    private final Map<String, Integer> offsets = new LinkedHashMap<>();
    private final int recordSize;

    public Layout(Schema schema) {
        this.schema = schema;
        int pos = 0;
        for (var e : schema.fields().entrySet()) {
            String name = e.getKey();
            Schema.FieldDef d = e.getValue();
            offsets.put(name, pos);
            if (d.type == FieldType.INT) {
                pos += Integer.BYTES; // 4B
            } else {
                // 文字列： [len:int][bytes(max)]
                // maxBytes を「UTF-8でmaxChars文字」の最悪想定で = maxChars * 4 として確保
                int maxBytes = Math.max(1, d.maxChars) * 4;
                pos += Integer.BYTES + maxBytes;
            }
        }
        this.recordSize = pos;
    }

    public Schema schema() {
        return schema;
    }

    public int offset(String field) {
        return offsets.get(field);
    }

    public int recordSize() {
        return recordSize;
    }

    /** 文字列フィールドの最大バイト数（len領域を除く） */
    public int maxStringBytes(String field) {
        var def = schema.fields().get(field);
        if (def.type != FieldType.STRING)
            throw new IllegalArgumentException("not string");
        return Math.max(1, def.maxChars) * 4;
    }
}
