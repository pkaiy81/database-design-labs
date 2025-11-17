package app.tx.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for WaitForGraph.
 */
class WaitForGraphTest {
    
    private WaitForGraph graph;
    
    @BeforeEach
    void setUp() {
        graph = new WaitForGraph();
    }
    
    @Test
    void testEmptyGraph() {
        assertTrue(graph.isEmpty());
        assertEquals(0, graph.size());
        assertTrue(graph.detectCycle().isEmpty());
    }
    
    @Test
    void testAddAndRemoveEdge() {
        graph.addEdge(1, 2);
        assertFalse(graph.isEmpty());
        assertEquals(1, graph.size());
        
        Set<Integer> waiting = graph.getWaitingFor(1);
        assertEquals(1, waiting.size());
        assertTrue(waiting.contains(2));
        
        graph.removeEdge(1, 2);
        assertTrue(graph.isEmpty());
    }
    
    @Test
    void testMultipleEdgesFromSameTx() {
        graph.addEdge(1, 2);
        graph.addEdge(1, 3);
        
        Set<Integer> waiting = graph.getWaitingFor(1);
        assertEquals(2, waiting.size());
        assertTrue(waiting.contains(2));
        assertTrue(waiting.contains(3));
    }
    
    @Test
    void testSimpleCycle_TwoTransactions() {
        // Tx1 → Tx2 → Tx1 (cycle)
        graph.addEdge(1, 2);
        graph.addEdge(2, 1);
        
        List<Integer> cycle = graph.detectCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(3, cycle.size()); // [1, 2, 1] or [2, 1, 2]
        
        // Verify it's a valid cycle
        assertTrue(cycle.contains(1));
        assertTrue(cycle.contains(2));
    }
    
    @Test
    void testSimpleCycle_ThreeTransactions() {
        // Tx1 → Tx2 → Tx3 → Tx1 (cycle)
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        graph.addEdge(3, 1);
        
        List<Integer> cycle = graph.detectCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(4, cycle.size()); // [1, 2, 3, 1] or similar
    }
    
    @Test
    void testNoCycle_Chain() {
        // Tx1 → Tx2 → Tx3 (no cycle)
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        
        List<Integer> cycle = graph.detectCycle();
        assertTrue(cycle.isEmpty(), "No cycle should be detected in a chain");
    }
    
    @Test
    void testRemoveTransaction() {
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        graph.addEdge(3, 1);
        
        // Remove Tx2 from the cycle
        graph.removeTransaction(2);
        
        // Now there should be no cycle
        List<Integer> cycle = graph.detectCycle();
        assertTrue(cycle.isEmpty(), "Cycle should be broken after removing Tx2");
        
        // Tx1 should still be waiting for Tx2, but Tx2 is removed
        // Actually, removeTransaction also removes Tx2 as a holder
        Set<Integer> tx1Waiting = graph.getWaitingFor(1);
        assertFalse(tx1Waiting.contains(2), "Tx2 should be removed as a holder");
    }
    
    @Test
    void testComplexGraph() {
        // Create a complex graph with multiple paths
        graph.addEdge(1, 2);
        graph.addEdge(1, 3);
        graph.addEdge(2, 4);
        graph.addEdge(3, 4);
        graph.addEdge(4, 5);
        
        // No cycle yet
        assertTrue(graph.detectCycle().isEmpty());
        
        // Add edge to create a cycle: Tx5 → Tx1
        graph.addEdge(5, 1);
        
        // Now there should be a cycle
        List<Integer> cycle = graph.detectCycle();
        assertFalse(cycle.isEmpty());
    }
    
    @Test
    void testSelfCycle() {
        // A transaction waiting for itself (shouldn't happen in practice)
        graph.addEdge(1, 1);
        
        List<Integer> cycle = graph.detectCycle();
        assertFalse(cycle.isEmpty());
        assertEquals(2, cycle.size());
        assertEquals(1, cycle.get(0));
        assertEquals(1, cycle.get(1));
    }
    
    @Test
    void testMultipleCycles_DetectsOne() {
        // Create two separate cycles
        // Cycle 1: Tx1 → Tx2 → Tx1
        graph.addEdge(1, 2);
        graph.addEdge(2, 1);
        
        // Cycle 2: Tx3 → Tx4 → Tx3
        graph.addEdge(3, 4);
        graph.addEdge(4, 3);
        
        // Should detect at least one cycle
        List<Integer> cycle = graph.detectCycle();
        assertFalse(cycle.isEmpty());
    }
    
    @Test
    void testToString() {
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);
        
        String str = graph.toString();
        assertNotNull(str);
        assertTrue(str.contains("Tx1"));
        assertTrue(str.contains("Tx2"));
    }
}
