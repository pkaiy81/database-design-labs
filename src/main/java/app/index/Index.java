package app.index;

public interface Index extends AutoCloseable {
    void open();

    void beforeFirst(SearchKey key); // 等値検索の開始位置へ

    boolean next(); // 次のインデックスエントリへ（同一キー内）

    RID getDataRid(); // 現在のデータRID

    void insert(SearchKey key, RID rid);

    void delete(SearchKey key, RID rid);

    RangeCursor range(SearchKey low, boolean lowInc,
            SearchKey high, boolean highInc);

    @Override
    void close();
}
