package app.index;

// 範囲スキャン用の軽量カーソル（次ステップで実体書き換え可）
public interface RangeCursor extends AutoCloseable {
    boolean next(); // 次の (key, rid) へ

    RID getDataRid();

    @Override
    void close();
}
