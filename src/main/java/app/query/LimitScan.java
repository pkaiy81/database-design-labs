package app.query;

import java.util.Objects;

public final class LimitScan implements Scan {
    private final Scan child;
    private final int limit;
    private int seen = 0;

    public LimitScan(Scan child, int limit) {
        this.child = Objects.requireNonNull(child);
        if (limit < 0)
            throw new IllegalArgumentException("limit must be >= 0");
        this.limit = limit;
    }

    @Override
    public void beforeFirst() {
        child.beforeFirst();
        seen = 0;
    }

    @Override
    public boolean next() {
        if (seen >= limit)
            return false;
        boolean ok = child.next();
        if (ok)
            seen++;
        return ok;
    }

    @Override
    public int getInt(String field) {
        return child.getInt(field);
    }

    @Override
    public String getString(String field) {
        return child.getString(field);
    }

    @Override
    public void close() {
        child.close();
    }
}
