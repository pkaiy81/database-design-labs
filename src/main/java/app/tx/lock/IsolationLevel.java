package app.tx.lock;

/**
 * Transaction isolation levels as defined by the SQL standard.
 * 
 * <p>
 * Each isolation level provides different guarantees about concurrent
 * transaction behavior:
 * 
 * <h3>READ_UNCOMMITTED</h3>
 * <ul>
 * <li><b>Dirty Read:</b> ❌ Allowed - can read uncommitted data from other
 * transactions</li>
 * <li><b>Non-repeatable Read:</b> ❌ Allowed - same query may return different
 * results</li>
 * <li><b>Phantom Read:</b> ❌ Allowed - new rows may appear in range
 * queries</li>
 * <li><b>Locking:</b> No read locks, only write locks</li>
 * <li><b>Performance:</b> Highest (but lowest consistency)</li>
 * </ul>
 * 
 * <h3>READ_COMMITTED (Default)</h3>
 * <ul>
 * <li><b>Dirty Read:</b> ✅ Prevented - only reads committed data</li>
 * <li><b>Non-repeatable Read:</b> ❌ Allowed</li>
 * <li><b>Phantom Read:</b> ❌ Allowed</li>
 * <li><b>Locking:</b> Short read locks (released immediately after read)</li>
 * <li><b>Performance:</b> Good balance</li>
 * </ul>
 * 
 * <h3>REPEATABLE_READ</h3>
 * <ul>
 * <li><b>Dirty Read:</b> ✅ Prevented</li>
 * <li><b>Non-repeatable Read:</b> ✅ Prevented - same query returns same
 * results</li>
 * <li><b>Phantom Read:</b> ❌ Allowed</li>
 * <li><b>Locking:</b> Hold all read locks until commit</li>
 * <li><b>Performance:</b> Lower due to more lock contention</li>
 * </ul>
 * 
 * <h3>SERIALIZABLE</h3>
 * <ul>
 * <li><b>Dirty Read:</b> ✅ Prevented</li>
 * <li><b>Non-repeatable Read:</b> ✅ Prevented</li>
 * <li><b>Phantom Read:</b> ✅ Prevented - no new rows in range queries</li>
 * <li><b>Locking:</b> Predicate locks or range locks</li>
 * <li><b>Performance:</b> Lowest (but highest consistency)</li>
 * </ul>
 * 
 * <p>
 * <b>Example Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * Tx tx = new Tx(IsolationLevel.REPEATABLE_READ);
 * // All reads will hold locks until commit
 * int value1 = tx.getInt(block, 0);
 * // ... other operations ...
 * int value2 = tx.getInt(block, 0); // Guaranteed to be same as value1
 * tx.commit(); // Locks released here
 * }</pre>
 * 
 * @author MiniDB Team
 */
public enum IsolationLevel {

    /**
     * Read Uncommitted - Allows dirty reads.
     * No read locks are acquired. Only write locks are held.
     */
    READ_UNCOMMITTED(0, "READ UNCOMMITTED", false, false, false),

    /**
     * Read Committed - Prevents dirty reads (default level).
     * Short read locks are acquired and released immediately after the read.
     */
    READ_COMMITTED(1, "READ COMMITTED", true, false, false),

    /**
     * Repeatable Read - Prevents dirty reads and non-repeatable reads.
     * Read locks are held until the transaction commits.
     */
    REPEATABLE_READ(2, "REPEATABLE READ", true, true, false),

    /**
     * Serializable - Prevents all anomalies including phantom reads.
     * Uses predicate locks or range locks to prevent phantoms.
     */
    SERIALIZABLE(3, "SERIALIZABLE", true, true, true);

    /** Numeric level (higher = stricter) */
    private final int level;

    /** SQL standard name */
    private final String sqlName;

    /** Whether to acquire read locks */
    private final boolean useReadLocks;

    /** Whether to hold read locks until commit (vs release immediately) */
    private final boolean holdReadLocks;

    /** Whether to use predicate/range locks to prevent phantoms */
    private final boolean usePredicateLocks;

    /**
     * Constructor for isolation level.
     * 
     * @param level             numeric level (0-3)
     * @param sqlName           SQL standard name
     * @param useReadLocks      whether to acquire read locks
     * @param holdReadLocks     whether to hold read locks until commit
     * @param usePredicateLocks whether to use predicate locks
     */
    IsolationLevel(int level, String sqlName, boolean useReadLocks,
            boolean holdReadLocks, boolean usePredicateLocks) {
        this.level = level;
        this.sqlName = sqlName;
        this.useReadLocks = useReadLocks;
        this.holdReadLocks = holdReadLocks;
        this.usePredicateLocks = usePredicateLocks;
    }

    /**
     * Gets the numeric level (0-3, higher = stricter).
     * 
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * Gets the SQL standard name.
     * 
     * @return the SQL name (e.g., "READ COMMITTED")
     */
    public String getSqlName() {
        return sqlName;
    }

    /**
     * Checks if this isolation level requires read locks.
     * 
     * @return true if read locks should be acquired
     */
    public boolean usesReadLocks() {
        return useReadLocks;
    }

    /**
     * Checks if this isolation level holds read locks until commit.
     * 
     * <p>
     * If false, read locks are released immediately after the read (READ_COMMITTED
     * behavior).
     * If true, read locks are held until commit/rollback (REPEATABLE_READ and
     * SERIALIZABLE).
     * 
     * @return true if read locks should be held until commit
     */
    public boolean holdsReadLocks() {
        return holdReadLocks;
    }

    /**
     * Checks if this isolation level uses predicate/range locks.
     * 
     * <p>
     * This is true only for SERIALIZABLE, which prevents phantom reads.
     * 
     * @return true if predicate locks should be used
     */
    public boolean usesPredicateLocks() {
        return usePredicateLocks;
    }

    /**
     * Checks if this isolation level prevents dirty reads.
     * 
     * @return true if dirty reads are prevented
     */
    public boolean preventsDirtyReads() {
        return useReadLocks;
    }

    /**
     * Checks if this isolation level prevents non-repeatable reads.
     * 
     * @return true if non-repeatable reads are prevented
     */
    public boolean preventsNonRepeatableReads() {
        return holdReadLocks;
    }

    /**
     * Checks if this isolation level prevents phantom reads.
     * 
     * @return true if phantom reads are prevented
     */
    public boolean preventsPhantomReads() {
        return usePredicateLocks;
    }

    /**
     * Returns the SQL standard name.
     * 
     * @return the SQL name
     */
    @Override
    public String toString() {
        return sqlName;
    }

    /**
     * Parses an isolation level from a SQL name.
     * 
     * @param sqlName the SQL name (case-insensitive)
     * @return the isolation level
     * @throws IllegalArgumentException if the name is not recognized
     */
    public static IsolationLevel fromSqlName(String sqlName) {
        String normalized = sqlName.toUpperCase().replace("_", " ");
        for (IsolationLevel level : values()) {
            if (level.sqlName.equals(normalized)) {
                return level;
            }
        }
        throw new IllegalArgumentException("Unknown isolation level: " + sqlName);
    }
}
