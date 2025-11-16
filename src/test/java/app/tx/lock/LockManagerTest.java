package app.tx.lock;

import app.storage.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LockManager クラスの単体テスト。
 */
@DisplayName("LockManager クラスのテスト")
class LockManagerTest {
    
    private LockTable lockTable;
    private LockManager lockMgr;
    private BlockId block1;
    private BlockId block2;
    
    @BeforeEach
    void setUp() {
        lockTable = new LockTable(1000);
        lockMgr = new LockManager(lockTable);
        block1 = new BlockId("test.tbl", 0);
        block2 = new BlockId("test.tbl", 1);
    }
    
    @Test
    @DisplayName("トランザクションが複数のブロックをロックできる")
    void testMultipleBlockLocking() {
        lockMgr.sLock(block1, 1);
        lockMgr.sLock(block2, 1);
        
        assertEquals(2, lockMgr.getLockCount(1));
        assertTrue(lockMgr.hasLock(block1, 1));
        assertTrue(lockMgr.hasLock(block2, 1));
    }
    
    @Test
    @DisplayName("release() がすべてのロックを解放する")
    void testReleaseAllLocks() {
        lockMgr.sLock(block1, 1);
        lockMgr.xLock(block2, 1);
        
        assertEquals(2, lockMgr.getLockCount(1));
        
        lockMgr.release(1);
        
        assertEquals(0, lockMgr.getLockCount(1));
        assertFalse(lockMgr.hasLock(block1, 1));
        assertFalse(lockMgr.hasLock(block2, 1));
    }
    
    @Test
    @DisplayName("複数のトランザクションが独立してロックを管理できる")
    void testMultipleTransactions() {
        lockMgr.sLock(block1, 1);
        lockMgr.sLock(block1, 2);
        
        assertTrue(lockMgr.hasLock(block1, 1));
        assertTrue(lockMgr.hasLock(block1, 2));
        
        lockMgr.release(1);
        
        assertFalse(lockMgr.hasLock(block1, 1));
        assertTrue(lockMgr.hasLock(block1, 2));  // トランザクション2は影響を受けない
    }
    
    @Test
    @DisplayName("同じブロックに対する重複ロック要求を処理できる")
    void testDuplicateLockRequest() {
        lockMgr.sLock(block1, 1);
        lockMgr.sLock(block1, 1);  // 2回目
        
        assertEquals(1, lockMgr.getLockCount(1));  // カウントは1のまま
    }
    
    @Test
    @DisplayName("共有ロックから排他ロックへのアップグレード")
    void testLockUpgrade() {
        lockMgr.sLock(block1, 1);
        assertTrue(lockMgr.hasLock(block1, 1));
        
        lockMgr.xLock(block1, 1);  // アップグレード
        assertTrue(lockMgr.hasLock(block1, 1));
        assertEquals(1, lockMgr.getLockCount(1));
    }
    
    @Test
    @DisplayName("getLockedBlocks() が保持しているブロックのセットを返す")
    void testGetLockedBlocks() {
        lockMgr.sLock(block1, 1);
        lockMgr.xLock(block2, 1);
        
        Set<BlockId> blocks = lockMgr.getLockedBlocks(1);
        assertEquals(2, blocks.size());
        assertTrue(blocks.contains(block1));
        assertTrue(blocks.contains(block2));
    }
    
    @Test
    @DisplayName("存在しないトランザクションの情報取得は安全")
    void testNonexistentTransaction() {
        assertEquals(0, lockMgr.getLockCount(999));
        assertFalse(lockMgr.hasLock(block1, 999));
        assertTrue(lockMgr.getLockedBlocks(999).isEmpty());
    }
    
    @Test
    @DisplayName("getTransactionCount() がトランザクション数を返す")
    void testGetTransactionCount() {
        assertEquals(0, lockMgr.getTransactionCount());
        
        lockMgr.sLock(block1, 1);
        assertEquals(1, lockMgr.getTransactionCount());
        
        lockMgr.sLock(block1, 2);
        assertEquals(2, lockMgr.getTransactionCount());
        
        lockMgr.release(1);
        assertEquals(1, lockMgr.getTransactionCount());
    }
    
    @Test
    @DisplayName("toString() がロックマネージャーの状態を表示する")
    void testToString() {
        lockMgr.sLock(block1, 1);
        lockMgr.xLock(block2, 2);
        
        String state = lockMgr.toString();
        assertTrue(state.contains("LockManager"));
        assertTrue(state.contains("Tx1"));
        assertTrue(state.contains("Tx2"));
    }
}
