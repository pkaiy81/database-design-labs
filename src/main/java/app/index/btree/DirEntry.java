package app.index.btree;

final class DirEntry {
    final int sepKey;   // 右ページの最小キー
    final int childBlk; // 右ページの blockNo

    DirEntry(int sepKey, int childBlk){
        this.sepKey = sepKey;
        this.childBlk = childBlk;
    }
}

