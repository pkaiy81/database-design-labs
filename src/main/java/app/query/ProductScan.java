package app.query;

import java.util.Objects;

/** 二重ループの直積スキャン。結合はこの上に SelectScan(eqField(...)) を被せて表現。 */
public final class ProductScan implements Scan {
    private final Scan left;
    private final Scan right;
    private boolean leftHasRow;

    public ProductScan(Scan left, Scan right) {
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    public void beforeFirst() {
        left.beforeFirst();
        right.beforeFirst();
        leftHasRow = false;
    }

    @Override
    public boolean next() {
        if (!leftHasRow) {
            if (!(leftHasRow = left.next()))
                return false;
        }
        while (true) {
            if (right.next()) {
                return true; // (left,current) × (right,current)
            } else {
                right.beforeFirst();
                if (!(leftHasRow = left.next()))
                    return false;
            }
        }
    }

    @Override
    public int getInt(String field) {
        try {
            return left.getInt(field);
        } catch (Exception ignore) {
            return right.getInt(field);
        }
    }

    @Override
    public String getString(String field) {
        try {
            return left.getString(field);
        } catch (Exception ignore) {
            return right.getString(field);
        }
    }

    @Override
    public void close() {
        try {
            left.close();
        } finally {
            right.close();
        }
    }
}
