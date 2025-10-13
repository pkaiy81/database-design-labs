package app.query;

import java.util.Objects;

public final class SelectScan implements Scan {
    private final Scan s;
    private final Predicate pred;

    public SelectScan(Scan s, Predicate pred) {
        this.s = Objects.requireNonNull(s);
        this.pred = Objects.requireNonNull(pred);
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        while (s.next()) {
            if (pred.evaluate(s))
                return true;
        }
        return false;
    }

    @Override
    public int getInt(String field) {
        return s.getInt(field);
    }

    @Override
    public String getString(String field) {
        return s.getString(field);
    }

    @Override
    public void close() {
        s.close();
    }
}
