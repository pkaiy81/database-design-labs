package app.storage;

import java.util.Objects;

/** ファイル名＋ブロック番号(0始まり)で1ページを一意に特定する値オブジェクト */
public final class BlockId {
    private final String filename;
    private final int number;

    public BlockId(String filename, int number) {
        this.filename = Objects.requireNonNull(filename, "filename");
        if (number < 0)
            throw new IllegalArgumentException("block number must be >= 0");
        this.number = number;
    }

    public String filename() {
        return filename;
    }

    public int number() {
        return number;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof BlockId))
            return false;
        BlockId other = (BlockId) o;
        return number == other.number && filename.equals(other.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, number);
    }

    @Override
    public String toString() {
        return filename + "[" + number + "]";
    }
}
