package app.tx.lock;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Wait-For Graph for deadlock detection.
 * 
 * <p>This class maintains a directed graph where:
 * <ul>
 *   <li>Nodes represent transactions (transaction IDs)</li>
 *   <li>An edge from Tx A to Tx B means "A is waiting for B to release a lock"</li>
 *   <li>A cycle in the graph indicates a deadlock</li>
 * </ul>
 * 
 * <p>Example:
 * <pre>
 * Tx1 → Tx2: Tx1 is waiting for a lock held by Tx2
 * Tx2 → Tx3: Tx2 is waiting for a lock held by Tx3
 * Tx3 → Tx1: Tx3 is waiting for a lock held by Tx1 → DEADLOCK (cycle detected)
 * </pre>
 * 
 * <p>Thread-safety: This class is thread-safe using ConcurrentHashMap.
 * 
 * @author MiniDB Team
 */
public class WaitForGraph {
    
    /**
     * The wait-for graph structure.
     * Key: Transaction ID (waiter)
     * Value: Set of transaction IDs that the waiter is waiting for
     */
    private final Map<Integer, Set<Integer>> graph;
    
    /**
     * Creates a new empty Wait-For Graph.
     */
    public WaitForGraph() {
        this.graph = new ConcurrentHashMap<>();
    }
    
    /**
     * Adds a wait-for edge to the graph.
     * 
     * <p>This indicates that {@code waiter} is now waiting for {@code holder} to release a lock.
     * 
     * @param waiter the transaction ID that is waiting
     * @param holder the transaction ID that holds the lock
     */
    public void addEdge(int waiter, int holder) {
        graph.computeIfAbsent(waiter, k -> ConcurrentHashMap.newKeySet()).add(holder);
    }
    
    /**
     * Removes a specific wait-for edge from the graph.
     * 
     * <p>This should be called when {@code waiter} stops waiting for {@code holder}
     * (e.g., when the lock is acquired or the wait is aborted).
     * 
     * @param waiter the transaction ID that was waiting
     * @param holder the transaction ID that held the lock
     */
    public void removeEdge(int waiter, int holder) {
        Set<Integer> holders = graph.get(waiter);
        if (holders != null) {
            holders.remove(holder);
            if (holders.isEmpty()) {
                graph.remove(waiter);
            }
        }
    }
    
    /**
     * Removes all edges where the given transaction is the waiter.
     * 
     * <p>This should be called when a transaction commits, rolls back, or is aborted.
     * 
     * @param txNum the transaction ID to remove as a waiter
     */
    public void removeTransaction(int txNum) {
        graph.remove(txNum);
        
        // Also remove this transaction as a holder from all other transactions
        for (Set<Integer> holders : graph.values()) {
            holders.remove(txNum);
        }
        
        // Clean up empty entries
        graph.entrySet().removeIf(entry -> entry.getValue().isEmpty());
    }
    
    /**
     * Detects if there is a cycle in the wait-for graph starting from any node.
     * 
     * <p>This method performs a depth-first search (DFS) from each node to detect cycles.
     * A cycle indicates a deadlock.
     * 
     * @return a list of transaction IDs involved in the deadlock cycle, or empty if no cycle exists
     */
    public List<Integer> detectCycle() {
        Set<Integer> visited = new HashSet<>();
        Set<Integer> recStack = new HashSet<>();
        Deque<Integer> path = new ArrayDeque<>();
        
        for (Integer txNum : graph.keySet()) {
            if (!visited.contains(txNum)) {
                List<Integer> cycle = detectCycleDFS(txNum, visited, recStack, path);
                if (cycle != null) {
                    return cycle;
                }
            }
        }
        
        return Collections.emptyList();
    }
    
    /**
     * Depth-first search to detect cycles.
     * 
     * @param current the current node being visited
     * @param visited set of all visited nodes
     * @param recStack set of nodes in the current recursion stack (for cycle detection)
     * @param path the current path being explored
     * @return the cycle path if found, null otherwise
     */
    private List<Integer> detectCycleDFS(int current, Set<Integer> visited, 
                                         Set<Integer> recStack, Deque<Integer> path) {
        visited.add(current);
        recStack.add(current);
        path.addLast(current);
        
        Set<Integer> neighbors = graph.get(current);
        if (neighbors != null) {
            for (Integer neighbor : neighbors) {
                if (!visited.contains(neighbor)) {
                    // Continue DFS
                    List<Integer> cycle = detectCycleDFS(neighbor, visited, recStack, path);
                    if (cycle != null) {
                        return cycle;
                    }
                } else if (recStack.contains(neighbor)) {
                    // Cycle detected!
                    // Extract the cycle from the path
                    List<Integer> cycle = new ArrayList<>();
                    boolean inCycle = false;
                    for (Integer node : path) {
                        if (node == neighbor) {
                            inCycle = true;
                        }
                        if (inCycle) {
                            cycle.add(node);
                        }
                    }
                    cycle.add(neighbor); // Complete the cycle
                    return cycle;
                }
            }
        }
        
        path.removeLast();
        recStack.remove(current);
        return null;
    }
    
    /**
     * Returns the number of transactions currently in the wait-for graph.
     * 
     * @return the number of waiting transactions
     */
    public int size() {
        return graph.size();
    }
    
    /**
     * Checks if the graph is empty.
     * 
     * @return true if there are no waiting transactions
     */
    public boolean isEmpty() {
        return graph.isEmpty();
    }
    
    /**
     * Gets all transactions that the given transaction is waiting for.
     * 
     * @param txNum the transaction ID
     * @return a set of transaction IDs that the given transaction is waiting for, or empty set
     */
    public Set<Integer> getWaitingFor(int txNum) {
        Set<Integer> holders = graph.get(txNum);
        return holders != null ? new HashSet<>(holders) : Collections.emptySet();
    }
    
    /**
     * Returns a string representation of the wait-for graph.
     * 
     * @return a human-readable representation of the graph
     */
    @Override
    public String toString() {
        if (graph.isEmpty()) {
            return "WaitForGraph: empty";
        }
        
        StringBuilder sb = new StringBuilder("WaitForGraph:\n");
        for (Map.Entry<Integer, Set<Integer>> entry : graph.entrySet()) {
            sb.append("  Tx").append(entry.getKey())
              .append(" → ").append(entry.getValue())
              .append("\n");
        }
        return sb.toString();
    }
}
