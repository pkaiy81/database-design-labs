package app.index;

import app.storage.BlockId;
import java.util.Objects;

public final class RID {
    private final BlockId block;
    private final int slot;

    public RID(BlockId block, int slot) {
        this.block = Objects.requireNonNull(block);
        if (slot < 0)
            throw new IllegalArgumentException("slot must be >= 0");
        this.slot = slot;
    }

    public BlockId block() {
        return block;
    }

    public int slot() {
        return slot;
    }

    @Override
    public String toString() {
        return block + "#slot=" + slot;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof RID))
            return false;
        RID r = (RID) o;
        return slot == r.slot && block.equals(r.block);
    }

    @Override
    public int hashCode() {
        return Objects.hash(block, slot);
    }
}
