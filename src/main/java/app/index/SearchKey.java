package app.index;

import java.util.Objects;

public final class SearchKey implements Comparable<SearchKey> {
    // まずは INT 専用で開始（後に拡張）
    private final int intKey;

    public static SearchKey ofInt(int v) {
        return new SearchKey(v);
    }

    private SearchKey(int v) {
        this.intKey = v;
    }

    public int asInt() {
        return intKey;
    }

    @Override
    public int compareTo(SearchKey o) {
        return Integer.compare(this.intKey, o.intKey);
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof SearchKey k) && k.intKey == this.intKey;
    }

    @Override
    public int hashCode() {
        return Objects.hash(intKey);
    }

    @Override
    public String toString() {
        return "Key(" + intKey + ")";
    }
}
