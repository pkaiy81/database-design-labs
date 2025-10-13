package app.query;

import java.util.*;

public final class ProjectScan implements Scan {
    private final Scan s;
    private final Set<String> fields;

    public ProjectScan(Scan s, Collection<String> fields) {
        this.s = Objects.requireNonNull(s);
        this.fields = new LinkedHashSet<>(fields);
    }

    @Override
    public void beforeFirst() {
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        return s.next();
    }

    @Override
    public int getInt(String field) {
        ensure(field);
        return s.getInt(field);
    }

    @Override
    public String getString(String field) {
        ensure(field);
        return s.getString(field);
    }

    private void ensure(String field) {
        if (!fields.contains(field)) {
            throw new IllegalArgumentException("field not projected: " + field);
        }
    }

    @Override
    public void close() {
        s.close();
    }
}
