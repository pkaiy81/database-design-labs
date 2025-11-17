package app.tx.lock;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for IsolationLevel enum.
 */
class IsolationLevelTest {

    @Test
    void testLevels() {
        assertEquals(0, IsolationLevel.READ_UNCOMMITTED.getLevel());
        assertEquals(1, IsolationLevel.READ_COMMITTED.getLevel());
        assertEquals(2, IsolationLevel.REPEATABLE_READ.getLevel());
        assertEquals(3, IsolationLevel.SERIALIZABLE.getLevel());
    }

    @Test
    void testSqlNames() {
        assertEquals("READ UNCOMMITTED", IsolationLevel.READ_UNCOMMITTED.getSqlName());
        assertEquals("READ COMMITTED", IsolationLevel.READ_COMMITTED.getSqlName());
        assertEquals("REPEATABLE READ", IsolationLevel.REPEATABLE_READ.getSqlName());
        assertEquals("SERIALIZABLE", IsolationLevel.SERIALIZABLE.getSqlName());
    }

    @Test
    void testReadUncommitted() {
        IsolationLevel level = IsolationLevel.READ_UNCOMMITTED;

        assertFalse(level.usesReadLocks(), "READ_UNCOMMITTED should not use read locks");
        assertFalse(level.holdsReadLocks());
        assertFalse(level.usesPredicateLocks());
        assertFalse(level.preventsDirtyReads());
        assertFalse(level.preventsNonRepeatableReads());
        assertFalse(level.preventsPhantomReads());
    }

    @Test
    void testReadCommitted() {
        IsolationLevel level = IsolationLevel.READ_COMMITTED;

        assertTrue(level.usesReadLocks(), "READ_COMMITTED should use read locks");
        assertFalse(level.holdsReadLocks(), "READ_COMMITTED should not hold read locks");
        assertFalse(level.usesPredicateLocks());
        assertTrue(level.preventsDirtyReads());
        assertFalse(level.preventsNonRepeatableReads());
        assertFalse(level.preventsPhantomReads());
    }

    @Test
    void testRepeatableRead() {
        IsolationLevel level = IsolationLevel.REPEATABLE_READ;

        assertTrue(level.usesReadLocks());
        assertTrue(level.holdsReadLocks(), "REPEATABLE_READ should hold read locks");
        assertFalse(level.usesPredicateLocks());
        assertTrue(level.preventsDirtyReads());
        assertTrue(level.preventsNonRepeatableReads());
        assertFalse(level.preventsPhantomReads());
    }

    @Test
    void testSerializable() {
        IsolationLevel level = IsolationLevel.SERIALIZABLE;

        assertTrue(level.usesReadLocks());
        assertTrue(level.holdsReadLocks());
        assertTrue(level.usesPredicateLocks(), "SERIALIZABLE should use predicate locks");
        assertTrue(level.preventsDirtyReads());
        assertTrue(level.preventsNonRepeatableReads());
        assertTrue(level.preventsPhantomReads());
    }

    @Test
    void testFromSqlName() {
        assertEquals(IsolationLevel.READ_UNCOMMITTED,
                IsolationLevel.fromSqlName("READ UNCOMMITTED"));
        assertEquals(IsolationLevel.READ_COMMITTED,
                IsolationLevel.fromSqlName("READ COMMITTED"));
        assertEquals(IsolationLevel.REPEATABLE_READ,
                IsolationLevel.fromSqlName("REPEATABLE READ"));
        assertEquals(IsolationLevel.SERIALIZABLE,
                IsolationLevel.fromSqlName("SERIALIZABLE"));
    }

    @Test
    void testFromSqlName_CaseInsensitive() {
        assertEquals(IsolationLevel.READ_COMMITTED,
                IsolationLevel.fromSqlName("read committed"));
        assertEquals(IsolationLevel.REPEATABLE_READ,
                IsolationLevel.fromSqlName("repeatable read"));
    }

    @Test
    void testFromSqlName_WithUnderscore() {
        assertEquals(IsolationLevel.READ_UNCOMMITTED,
                IsolationLevel.fromSqlName("READ_UNCOMMITTED"));
        assertEquals(IsolationLevel.REPEATABLE_READ,
                IsolationLevel.fromSqlName("REPEATABLE_READ"));
    }

    @Test
    void testFromSqlName_Invalid() {
        assertThrows(IllegalArgumentException.class,
                () -> IsolationLevel.fromSqlName("INVALID"));
    }

    @Test
    void testToString() {
        assertEquals("READ UNCOMMITTED", IsolationLevel.READ_UNCOMMITTED.toString());
        assertEquals("READ COMMITTED", IsolationLevel.READ_COMMITTED.toString());
        assertEquals("REPEATABLE READ", IsolationLevel.REPEATABLE_READ.toString());
        assertEquals("SERIALIZABLE", IsolationLevel.SERIALIZABLE.toString());
    }
}
