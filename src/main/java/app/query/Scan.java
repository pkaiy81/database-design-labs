package app.query;

public interface Scan extends AutoCloseable {
    void beforeFirst();

    boolean next();

    int getInt(String field);

    String getString(String field);

    @Override
    void close();
}
