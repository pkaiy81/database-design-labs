package app.tx.lock;

import app.storage.BlockId;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import static org.junit.jupiter.api.Assertions.*;

/**
 * LockTable クラスの単体テスト。
 */
@DisplayName("LockTable クラスのテスト")
class LockTableTest {
    
    private LockTable lockTable;
    private BlockId block1;
    private BlockId block2;
    
    @BeforeEach
    void setUp() {
        lockTable = new LockTable(1000);  // 1秒のタイムアウト
        block1 = new BlockId("test.tbl", 0);
        block2 = new BlockId("test.tbl", 1);
    }
    
    @Test
    @DisplayName("異なるブロックに対して独立したロックを管理できる")
    void testIndependentBlocks() {
        // block1 に排他ロック
        lockTable.xLock(block1, 1);
        
        // block2 には別のトランザクションがロックを取得できる
        assertDoesNotThrow(() -> lockTable.xLock(block2, 2));
        
        assertTrue(lockTable.isLocked(block1, 1));
        assertTrue(lockTable.isLocked(block2, 2));
    }
    
    @Test
    @DisplayName("ロックの取得と解放が正しく動作する")
    void testLockAndUnlock() {
        lockTable.sLock(block1, 1);
        assertTrue(lockTable.isLocked(block1, 1));
        
        lockTable.unlock(block1, 1);
        assertFalse(lockTable.isLocked(block1, 1));
    }
    
    @Test
    @DisplayName("複数のトランザクションが異なるブロックで並行動作できる")
    void testConcurrentAccessToDifferentBlocks() {
        assertDoesNotThrow(() -> {
            lockTable.xLock(block1, 1);
            lockTable.xLock(block2, 2);
        });
        
        assertTrue(lockTable.isLocked(block1, 1));
        assertTrue(lockTable.isLocked(block2, 2));
    }
    
    @Test
    @DisplayName("存在しないブロックのロック解放は安全に動作する")
    void testUnlockNonexistentBlock() {
        BlockId nonexistent = new BlockId("nonexistent.tbl", 99);
        assertDoesNotThrow(() -> lockTable.unlock(nonexistent, 1));
    }
    
    @Test
    @DisplayName("ロックテーブルのサイズが正しく管理される")
    void testLockTableSize() {
        assertEquals(0, lockTable.size());
        
        lockTable.sLock(block1, 1);
        assertEquals(1, lockTable.size());
        
        lockTable.sLock(block2, 2);
        assertEquals(2, lockTable.size());
    }
    
    @Test
    @DisplayName("toString() がロックテーブルの状態を表示する")
    void testToString() {
        lockTable.sLock(block1, 1);
        String state = lockTable.toString();
        
        assertTrue(state.contains("LockTable"));
        assertTrue(state.contains("timeout"));
    }
}
