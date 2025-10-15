package app.index.btree;

final class BTreeLayouts {
    private BTreeLayouts() {
    }

    // ページヘッダ（固定長）
    static final int OFF_FLAG = 0; // >0 : 内部ノードレベル, ==0 : 葉
    static final int OFF_COUNT = 4; // キー数
    static final int OFF_PARENT = 8; // 親 BlockNo (将来用; 今は未使用でも可)
    static final int OFF_PREV = 12; // 葉の前ページ BlockNo (-1 if none)
    static final int OFF_NEXT = 16; // 葉の次ページ BlockNo (-1 if none)
    static final int HEADER_SIZE = 20;

    // スロット：INTキー前提
    // 内部: [ key:int (4) | child:int (4) ] = 8 bytes
    static final int DIR_SLOT_SIZE = 8;
    static final int LEAF_SLOT_SIZE = 12; // [ key:int | rid.block:int | rid.slot:int ]
}
