package app.tx.lock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for DeadlockDetector.
 */
class DeadlockDetectorTest {

    private WaitForGraph graph;
    private DeadlockDetector detector;

    @BeforeEach
    void setUp() {
        graph = new WaitForGraph();
        detector = new DeadlockDetector(graph, 100); // 100ms interval for fast testing
    }

    @Test
    void testStartAndStop() {
        assertFalse(detector.isRunning());

        detector.start();
        assertTrue(detector.isRunning());

        detector.stop();
        assertFalse(detector.isRunning());
    }

    @Test
    void testCannotStartTwice() {
        detector.start();
        assertThrows(IllegalStateException.class, () -> detector.start());
        detector.stop();
    }

    @Test
    void testDetectsDeadlock() throws InterruptedException {
        AtomicInteger victimCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(1);

        detector.setAbortCallback(txNum -> {
            victimCount.incrementAndGet();
            latch.countDown();
        });

        detector.start();

        // Create a deadlock cycle: Tx1 → Tx2 → Tx1
        graph.addEdge(1, 2);
        graph.addEdge(2, 1);

        // Wait for detector to detect and resolve
        boolean detected = latch.await(2, TimeUnit.SECONDS);
        assertTrue(detected, "Deadlock should be detected within 2 seconds");

        detector.stop();

        assertEquals(1, victimCount.get(), "Exactly one transaction should be aborted");
        assertEquals(1, detector.getDeadlocksDetected());
        assertEquals(1, detector.getTransactionsAborted());
    }

    @Test
    void testNoCycleNoAbort() throws InterruptedException {
        AtomicInteger victimCount = new AtomicInteger(0);

        detector.setAbortCallback(txNum -> victimCount.incrementAndGet());
        detector.start();

        // Create a chain (no cycle): Tx1 → Tx2 → Tx3
        graph.addEdge(1, 2);
        graph.addEdge(2, 3);

        // Wait a bit to ensure detector runs
        Thread.sleep(300);

        detector.stop();

        assertEquals(0, victimCount.get(), "No transaction should be aborted (no cycle)");
        assertEquals(0, detector.getDeadlocksDetected());
    }

    @Test
    void testVictimSelection_SelectsHighestId() throws InterruptedException {
        AtomicInteger selectedVictim = new AtomicInteger(-1);
        CountDownLatch latch = new CountDownLatch(1);

        detector.setAbortCallback(txNum -> {
            selectedVictim.set(txNum);
            latch.countDown();
        });

        detector.start();

        // Create cycle: Tx5 → Tx10 → Tx5
        // Expected victim: Tx10 (highest ID)
        graph.addEdge(5, 10);
        graph.addEdge(10, 5);

        latch.await(2, TimeUnit.SECONDS);
        detector.stop();

        assertEquals(10, selectedVictim.get(), "Should select transaction with highest ID");
    }

    @Test
    void testMultipleDeadlocksDetected() throws InterruptedException {
        AtomicInteger victimCount = new AtomicInteger(0);

        detector.setAbortCallback(txNum -> {
            victimCount.incrementAndGet();
            // Remove the victim from the graph
            graph.removeTransaction(txNum);
        });

        detector.start();

        // Create first deadlock
        graph.addEdge(1, 2);
        graph.addEdge(2, 1);

        Thread.sleep(200); // Let it detect

        // Create second deadlock
        graph.addEdge(3, 4);
        graph.addEdge(4, 3);

        Thread.sleep(200); // Let it detect

        detector.stop();

        assertTrue(victimCount.get() >= 2, "At least 2 victims should be aborted");
        assertTrue(detector.getDeadlocksDetected() >= 2);
    }

    @Test
    void testEmptyGraphNoCrash() throws InterruptedException {
        detector.start();

        // Let detector run on empty graph
        Thread.sleep(300);

        detector.stop();

        assertEquals(0, detector.getDeadlocksDetected());
        assertEquals(0, detector.getTransactionsAborted());
    }
}
