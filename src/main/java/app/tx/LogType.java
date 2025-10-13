package app.tx;

public enum LogType {
    START(1), COMMIT(2), ROLLBACK(3), SET_INT(4);

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
