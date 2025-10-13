package app.index;

import app.record.*;
import app.storage.FileMgr;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/** 最小の索引レジストリ： (table, column) -> HashIndex */
public final class IndexRegistry {
    private final FileMgr fm;
    private final Map<Key, HashIndex> map = new ConcurrentHashMap<>();

    public IndexRegistry(FileMgr fm) {
        this.fm = fm;
    }

    public HashIndex createHashIndex(String table, String column, Layout layout, int bucketCount) {
        String indexName = table + "_" + column;
        String tableFile = table + ".tbl";
        HashIndex idx = new HashIndex(fm, indexName, tableFile, bucketCount);
        map.put(new Key(table, column), idx);
        return idx;
    }

    public Optional<HashIndex> findHashIndex(String table, String column) {
        return Optional.ofNullable(map.get(new Key(table, column)));
    }

    /** 既存テーブル全件を走査して索引を構築（INT列のみ） */
    public void buildHashIndex(String table, String column, TableFile tf, Layout layout) {
        HashIndex idx = Objects.requireNonNull(map.get(new Key(table, column)), "index not registered");
        try (TableScan scan = new TableScan(fm, tf)) {
            scan.beforeFirst();
            while (scan.next()) {
                int key = scan.getInt(column);
                RID rid = new RID(new app.storage.BlockId(tf.filename(), scan.currentBlockNumber()),
                        scan.currentSlot());
                idx.put(key, rid);
            }
        }
    }

    private static final class Key {
        final String t, c;

        Key(String t, String c) {
            this.t = t;
            this.c = c;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k && t.equals(k.t) && c.equals(k.c);
        }

        @Override
        public int hashCode() {
            return Objects.hash(t, c);
        }
    }
}
