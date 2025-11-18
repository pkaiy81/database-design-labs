package app.tx;

/**
 * ログレコードタイプ定義
 * Phase 1: START, COMMIT, ROLLBACK, SET_INT
 * Phase 2: SET_STRING, CHECKPOINT (追加)
 */
public enum LogType {
    START(1),
    COMMIT(2),
    ROLLBACK(3),
    SET_INT(4),
    SET_STRING(5), // Phase 2: 文字列更新のUNDOログ
    CHECKPOINT(6); // Phase 2: チェックポイントログ

    public final int code;

    LogType(int c) {
        this.code = c;
    }

    public static LogType from(int c) {
        for (var t : values())
            if (t.code == c)
                return t;
        throw new IllegalArgumentException("unknown log type: " + c);
    }
}
