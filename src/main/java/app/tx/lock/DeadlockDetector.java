package app.tx.lock;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Deadlock detector that periodically checks the Wait-For Graph for cycles
 * and automatically aborts victim transactions to resolve deadlocks.
 * 
 * <p>
 * <b>Design:</b>
 * </p>
 * <ul>
 * <li>Runs a background thread that periodically checks for deadlocks</li>
 * <li>When a deadlock is detected, selects a victim transaction</li>
 * <li>Aborts the victim by invoking a callback</li>
 * <li>Uses a configurable detection interval (default: 1 second)</li>
 * </ul>
 * 
 * <p>
 * <b>Victim Selection Strategy:</b>
 * </p>
 * <p>
 * Currently uses a simple strategy: select the transaction with the highest ID
 * (youngest transaction). This can be extended with more sophisticated
 * strategies
 * such as:
 * <ul>
 * <li>Number of locks held</li>
 * <li>Transaction age</li>
 * <li>Work done so far</li>
 * <li>Priority</li>
 * </ul>
 * 
 * <p>
 * <b>Usage:</b>
 * </p>
 * 
 * <pre>{@code
 * DeadlockDetector detector = new DeadlockDetector(waitForGraph);
 * detector.setAbortCallback(txNum -> {
 *     // Abort the transaction
 *     tx.rollback();
 * });
 * detector.start();
 * 
 * // ... later ...
 * detector.stop();
 * }</pre>
 * 
 * @author MiniDB Team
 */
public class DeadlockDetector {

    private static final Logger LOGGER = Logger.getLogger(DeadlockDetector.class.getName());

    /** Default detection interval in milliseconds */
    private static final long DEFAULT_DETECTION_INTERVAL_MS = 1000;

    /** The wait-for graph to monitor */
    private final WaitForGraph waitForGraph;

    /** Detection interval in milliseconds */
    private final long detectionIntervalMs;

    /** Executor service for the background detection thread */
    private ScheduledExecutorService executor;

    /** Flag indicating if the detector is running */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /** Callback to abort a transaction */
    private Consumer<Integer> abortCallback;

    /** Number of deadlocks detected */
    private volatile long deadlocksDetected = 0;

    /** Number of transactions aborted */
    private volatile long transactionsAborted = 0;

    /**
     * Creates a new deadlock detector with default detection interval.
     * 
     * @param waitForGraph the wait-for graph to monitor
     */
    public DeadlockDetector(WaitForGraph waitForGraph) {
        this(waitForGraph, DEFAULT_DETECTION_INTERVAL_MS);
    }

    /**
     * Creates a new deadlock detector with custom detection interval.
     * 
     * @param waitForGraph        the wait-for graph to monitor
     * @param detectionIntervalMs the detection interval in milliseconds
     */
    public DeadlockDetector(WaitForGraph waitForGraph, long detectionIntervalMs) {
        this.waitForGraph = waitForGraph;
        this.detectionIntervalMs = detectionIntervalMs;
    }

    /**
     * Sets the callback to invoke when a transaction needs to be aborted.
     * 
     * <p>
     * The callback receives the transaction ID as a parameter.
     * 
     * @param abortCallback the abort callback
     */
    public void setAbortCallback(Consumer<Integer> abortCallback) {
        this.abortCallback = abortCallback;
    }

    /**
     * Starts the deadlock detector.
     * 
     * <p>
     * This spawns a background thread that periodically checks for deadlocks.
     * 
     * @throws IllegalStateException if the detector is already running
     */
    public synchronized void start() {
        if (running.get()) {
            throw new IllegalStateException("DeadlockDetector is already running");
        }

        executor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "DeadlockDetector");
            t.setDaemon(true);
            return t;
        });

        executor.scheduleAtFixedRate(
                this::detectAndResolve,
                detectionIntervalMs,
                detectionIntervalMs,
                TimeUnit.MILLISECONDS);

        running.set(true);
        LOGGER.info("DeadlockDetector started with interval " + detectionIntervalMs + "ms");
    }

    /**
     * Stops the deadlock detector.
     * 
     * <p>
     * This shuts down the background thread gracefully.
     */
    public synchronized void stop() {
        if (!running.get()) {
            return;
        }

        running.set(false);

        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        LOGGER.info("DeadlockDetector stopped. Detected " + deadlocksDetected +
                " deadlocks, aborted " + transactionsAborted + " transactions");
    }

    /**
     * Checks if the detector is running.
     * 
     * @return true if running
     */
    public boolean isRunning() {
        return running.get();
    }

    /**
     * Gets the number of deadlocks detected.
     * 
     * @return the deadlock count
     */
    public long getDeadlocksDetected() {
        return deadlocksDetected;
    }

    /**
     * Gets the number of transactions aborted.
     * 
     * @return the abort count
     */
    public long getTransactionsAborted() {
        return transactionsAborted;
    }

    /**
     * Performs one cycle of deadlock detection and resolution.
     * 
     * <p>
     * This method:
     * <ol>
     * <li>Checks the wait-for graph for cycles</li>
     * <li>If a cycle is found, selects a victim</li>
     * <li>Aborts the victim via the callback</li>
     * </ol>
     */
    private void detectAndResolve() {
        try {
            List<Integer> cycle = waitForGraph.detectCycle();

            if (!cycle.isEmpty()) {
                deadlocksDetected++;

                LOGGER.warning("Deadlock detected! Cycle: " + cycle);

                // Select victim (transaction with highest ID = youngest)
                int victim = selectVictim(cycle);

                LOGGER.warning("Selected victim: Tx" + victim + " (will be aborted)");

                // Abort the victim
                if (abortCallback != null) {
                    abortCallback.accept(victim);
                    transactionsAborted++;
                } else {
                    LOGGER.severe("No abort callback set! Cannot resolve deadlock.");
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error during deadlock detection", e);
        }
    }

    /**
     * Selects a victim transaction from the deadlock cycle.
     * 
     * <p>
     * Current strategy: select the transaction with the highest ID (youngest).
     * This is a simple heuristic that tends to abort transactions that have
     * done less work.
     * 
     * @param cycle the list of transaction IDs in the deadlock cycle
     * @return the transaction ID of the victim
     */
    private int selectVictim(List<Integer> cycle) {
        // Simple strategy: abort the transaction with the highest ID (youngest)
        return cycle.stream().max(Integer::compareTo).orElse(cycle.get(0));
    }
}
