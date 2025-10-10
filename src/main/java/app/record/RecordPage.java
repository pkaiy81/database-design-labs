package app.record;

import app.storage.Page;

public final class RecordPage {
    private final Page page;
    private final Layout layout;
    private final int blockSize;
    private final int recordSize;
    private final int slots; // slotsPerPage
    private final int headerSize; // = slots

    public RecordPage(Page page, Layout layout, int blockSize) {
        this.page = page;
        this.layout = layout;
        this.blockSize = blockSize;
        this.recordSize = layout.recordSize();
        this.slots = blockSize / (recordSize + 1);
        this.headerSize = slots; // 1バイト/スロット
    }

    /** ページの全スロットを空（0）で初期化 */
    public void format() {
        var a = page.contents();
        for (int i = 0; i < headerSize; i++)
            a[i] = 0;
        // データ部はゼロのままでOK
    }

    public int slots() {
        return slots;
    }

    public boolean isUsed(int slot) {
        check(slot);
        return (page.contents()[slot] != 0);
    }

    public void setUsed(int slot, boolean used) {
        check(slot);
        page.contents()[slot] = (byte) (used ? 1 : 0);
    }

    public int offsetOf(int slot) {
        return headerSize + slot * recordSize;
    }

    private void check(int slot) {
        if (slot < 0 || slot >= slots)
            throw new IndexOutOfBoundsException();
    }

    /** 次の空きスロットを返す（なければ -1） */
    public int findFree() {
        for (int s = 0; s < slots; s++)
            if (!isUsed(s))
                return s;
        return -1;
    }

    /** slot 以降で次の使用中スロット（なければ -1） */
    public int nextUsed(int slot) {
        for (int s = slot + 1; s < slots; s++)
            if (isUsed(s))
                return s;
        return -1;
    }

    // ---- レコードのフィールド I/O ----
    public int getInt(int slot, String field) {
        int base = offsetOf(slot) + layout.offset(field);
        return page.getInt(base);
    }

    public void setInt(int slot, String field, int v) {
        int base = offsetOf(slot) + layout.offset(field);
        page.setInt(base, v);
    }

    public String getString(int slot, String field) {
        int base = offsetOf(slot) + layout.offset(field);
        return page.getString(base);
    }

    public void setString(int slot, String field, String s) {
        int base = offsetOf(slot) + layout.offset(field);
        // 最大バイト長に収まるように切り詰め（UTF-8換算 大まかに）
        int max = layout.maxStringBytes(field);
        byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if (bytes.length > max) {
            // 超過時はざっくり切り詰め（境界の整合性は後続で精密化可）
            s = new String(bytes, 0, max, java.nio.charset.StandardCharsets.UTF_8);
        }
        page.setString(base, s);
    }

    public Page page() {
        return page;
    }
}
