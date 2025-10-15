package app.index.btree;

final class DirEntry {
    final int sepKey; // 昇格キー(=右ページ先頭キー)
    final int childBlk; // その右ページの BlockNo

    DirEntry(int sepKey, int childBlk) {
        this.sepKey = sepKey;
        this.childBlk = childBlk;
    }
}
