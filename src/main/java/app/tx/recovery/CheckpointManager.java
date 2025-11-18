package app.tx.recovery;

import app.memory.LogManager;
import app.tx.LogCodec;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * CheckpointManager: チェックポイントマネージャ (Phase 2)
 * 
 * <p>
 * 役割:
 * <ul>
 * <li>アクティブトランザクションのリストを定期的にログに記録</li>
 * <li>クラッシュリカバリの効率化（チェックポイント以降のログのみ処理）</li>
 * <li>トランザクション開始・終了の追跡</li>
 * </ul>
 * 
 * <p>
 * チェックポイントアルゴリズム:
 * <ol>
 * <li>現在アクティブな全トランザクションのIDリストを取得</li>
 * <li>CHECKPOINTログレコードを書き込む（アクティブTxリスト付き）</li>
 * <li>ログをフラッシュしてディスクに永続化</li>
 * </ol>
 * 
 * <p>
 * 定期実行:
 * <ul>
 * <li>デフォルト: 30秒ごと</li>
 * <li>バックグラウンドスレッドで自動実行</li>
 * <li>start() で開始、stop() で停止</li>
 * </ul>
 * 
 * @see app.tx.LogType#CHECKPOINT
 * @see RecoveryManager
 */
public class CheckpointManager {
    private final LogManager logManager;
    private final Set<Integer> activeTxIds = ConcurrentHashMap.newKeySet();
    private ScheduledExecutorService scheduler;
    private final long intervalSeconds;

    /**
     * CheckpointManagerを作成（デフォルト: 30秒間隔）
     * 
     * @param logManager ログマネージャ
     */
    public CheckpointManager(LogManager logManager) {
        this(logManager, 30);
    }

    /**
     * CheckpointManagerを作成（間隔指定可能）
     * 
     * @param logManager      ログマネージャ
     * @param intervalSeconds チェックポイント実行間隔（秒）
     */
    public CheckpointManager(LogManager logManager, long intervalSeconds) {
        this.logManager = logManager;
        this.intervalSeconds = intervalSeconds;
    }

    /**
     * 定期的なチェックポイント実行を開始
     */
    public void start() {
        if (scheduler != null) {
            throw new IllegalStateException("CheckpointManager is already started");
        }

        scheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "CheckpointManager");
            t.setDaemon(true); // デーモンスレッド（メインスレッド終了時に自動終了）
            return t;
        });

        scheduler.scheduleAtFixedRate(
                this::checkpoint,
                intervalSeconds,
                intervalSeconds,
                TimeUnit.SECONDS);

        System.out.println("[CheckpointManager] Started with interval: " + intervalSeconds + "s");
    }

    /**
     * 定期的なチェックポイント実行を停止
     */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            scheduler = null;
            System.out.println("[CheckpointManager] Stopped");
        }
    }

    /**
     * トランザクション開始を記録
     * 
     * @param txId トランザクションID
     */
    public void registerTransaction(int txId) {
        activeTxIds.add(txId);
        System.out.println("[CheckpointManager] Registered tx " + txId + " (active count: " + activeTxIds.size() + ")");
    }

    /**
     * トランザクション終了を記録（commit/rollback）
     * 
     * @param txId トランザクションID
     */
    public void unregisterTransaction(int txId) {
        activeTxIds.remove(txId);
        System.out
                .println("[CheckpointManager] Unregistered tx " + txId + " (active count: " + activeTxIds.size() + ")");
    }

    /**
     * 現在のアクティブトランザクション数を取得
     * 
     * @return アクティブトランザクション数
     */
    public int getActiveTransactionCount() {
        return activeTxIds.size();
    }

    /**
     * チェックポイントを実行（手動実行も可能）
     * 
     * <p>
     * 処理内容:
     * <ol>
     * <li>現在のアクティブTxリストのスナップショットを取得</li>
     * <li>CHECKPOINTログレコードを書き込む</li>
     * <li>ログをフラッシュ</li>
     * </ol>
     */
    public void checkpoint() {
        List<Integer> snapshot = new ArrayList<>(activeTxIds);

        if (snapshot.isEmpty()) {
            System.out.println("[CheckpointManager] CHECKPOINT skipped (no active transactions)");
            return;
        }

        byte[] checkpointLog = LogCodec.checkpoint(snapshot);
        long lsn = logManager.append(checkpointLog);
        logManager.flush(lsn);

        System.out.println("[CheckpointManager] CHECKPOINT written with " + snapshot.size() +
                " active transactions: " + snapshot);
    }
}
