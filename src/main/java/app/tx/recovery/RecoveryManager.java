package app.tx.recovery;

import app.memory.BufferMgr;
import app.storage.BlockId;
import app.tx.LogCodec;
import app.tx.LogReader;

import java.io.File;
import java.util.*;

/**
 * RecoveryManager: リカバリマネージャ (Phase 2)
 * 
 * <p>
 * 役割:
 * <ul>
 * <li>システム起動時のクラッシュリカバリ（UNDO/REDO）</li>
 * <li>チェックポイントからの効率的なリカバリ</li>
 * <li>アクティブトランザクションのUNDO処理</li>
 * <li>コミット済みトランザクションのREDO処理（将来拡張）</li>
 * </ul>
 * 
 * <p>
 * リカバリアルゴリズム:
 * <ol>
 * <li>ログを後ろから前に読む（最新→最古）</li>
 * <li>CHECKPOINTを見つけたら、アクティブTxリストを取得</li>
 * <li>CHECKPOINTより古いログは、アクティブTxのみ処理</li>
 * <li>各トランザクションのSTARTまで辿り、全てUNDO</li>
 * </ol>
 * 
 * <p>
 * 現在の実装: UNDO-Only (Phase 1の延長)
 * <ul>
 * <li>SET_INT, SET_STRING の変更を元に戻す</li>
 * <li>COMMIT済みトランザクションは何もしない</li>
 * <li>REDO処理は未実装（Phase 2完全版で対応）</li>
 * </ul>
 * 
 * @see app.tx.LogType
 * @see app.tx.LogCodec
 */
public class RecoveryManager {
    private final File logDir;
    private final BufferMgr bufferMgr;

    /**
     * RecoveryManagerを作成
     * 
     * @param logDir    ログディレクトリ
     * @param bufferMgr バッファマネージャ（UNDO時のページ書き戻しに必要）
     */
    public RecoveryManager(File logDir, BufferMgr bufferMgr) {
        this.logDir = logDir;
        this.bufferMgr = bufferMgr;
    }

    /**
     * システム起動時のクラッシュリカバリを実行
     * 
     * <p>
     * アルゴリズム:
     * <ol>
     * <li>ログを最新から最古に向かってスキャン</li>
     * <li>コミット/ロールバック済みTxは記録（何もしない）</li>
     * <li>それ以外のTxは「アクティブ（未完了）」として記録</li>
     * <li>CHECKPOINTを見つけたら、そこに記録されたアクティブTxのみ処理対象にする</li>
     * <li>各アクティブTxについて、SET_INT/SET_STRINGをUNDO</li>
     * </ol>
     */
    public void recover() {
        System.out.println("[RecoveryManager] Starting crash recovery...");

        List<byte[]> allLogs = new LogReader(logDir.toPath()).readAll();
        if (allLogs.isEmpty()) {
            System.out.println("[RecoveryManager] No logs found. Clean start.");
            return;
        }

        // Phase 1: アクティブトランザクションの特定
        Set<Integer> committedOrRolledBack = new HashSet<>();
        Set<Integer> activeTxIds = new HashSet<>();
        Set<Integer> checkpointActiveTxIds = null;

        for (int i = allLogs.size() - 1; i >= 0; i--) {
            var parsed = LogCodec.parse(allLogs.get(i));

            switch (parsed.type) {
                case COMMIT:
                case ROLLBACK:
                    committedOrRolledBack.add(parsed.txId);
                    break;

                case CHECKPOINT:
                    // チェックポイント発見: これ以降のログはこのリストのTxのみ処理
                    checkpointActiveTxIds = new HashSet<>(parsed.activeTxIds);
                    System.out.println("[RecoveryManager] Found CHECKPOINT with active txs: " + checkpointActiveTxIds);
                    break;

                case START:
                    // STARTを見つけたら、コミット/ロールバックされていなければアクティブ
                    if (!committedOrRolledBack.contains(parsed.txId)) {
                        activeTxIds.add(parsed.txId);
                    }
                    break;

                default:
                    // SET_INT, SET_STRING等はSTART判定に使わない
                    break;
            }
        }

        // チェックポイントがあれば、それより古いログはチェックポイントのアクティブTxのみ処理
        Set<Integer> txsToUndo = new HashSet<>(activeTxIds);
        if (checkpointActiveTxIds != null) {
            txsToUndo.retainAll(checkpointActiveTxIds);
        }

        if (txsToUndo.isEmpty()) {
            System.out.println("[RecoveryManager] No active transactions to undo. Recovery complete.");
            return;
        }

        System.out.println("[RecoveryManager] Undoing transactions: " + txsToUndo);

        // Phase 2: アクティブトランザクションのUNDO（後ろから前へ）
        for (int i = allLogs.size() - 1; i >= 0; i--) {
            var parsed = LogCodec.parse(allLogs.get(i));

            if (!txsToUndo.contains(parsed.txId)) {
                continue; // このTxは処理対象外
            }

            switch (parsed.type) {
                case SET_INT:
                    undoSetInt(parsed);
                    break;

                case SET_STRING:
                    undoSetString(parsed);
                    break;

                case START:
                    // このTxのSTARTまで辿ったので、UNDO完了
                    txsToUndo.remove(parsed.txId);
                    System.out.println("[RecoveryManager] Tx " + parsed.txId + " rolled back.");
                    break;

                default:
                    // その他のログタイプは何もしない
                    break;
            }
        }

        System.out.println("[RecoveryManager] Crash recovery complete.");
    }

    /**
     * SET_INTログのUNDO処理
     */
    private void undoSetInt(LogCodec.Parsed parsed) {
        BlockId blockId = new BlockId(parsed.filename, parsed.blk);
        var buf = bufferMgr.pin(blockId);
        try {
            buf.contents().setInt(parsed.offset, parsed.oldValInt);
            buf.setDirty();
            buf.flushIfDirty();
            System.out.println("[RecoveryManager] UNDO SET_INT: tx=" + parsed.txId +
                    ", block=" + blockId + ", offset=" + parsed.offset +
                    ", oldVal=" + parsed.oldValInt);
        } finally {
            bufferMgr.unpin(buf);
        }
    }

    /**
     * SET_STRINGログのUNDO処理
     */
    private void undoSetString(LogCodec.Parsed parsed) {
        BlockId blockId = new BlockId(parsed.filename, parsed.blk);
        var buf = bufferMgr.pin(blockId);
        try {
            buf.contents().setString(parsed.offset, parsed.oldValString);
            buf.setDirty();
            buf.flushIfDirty();
            System.out.println("[RecoveryManager] UNDO SET_STRING: tx=" + parsed.txId +
                    ", block=" + blockId + ", offset=" + parsed.offset +
                    ", oldVal='" + parsed.oldValString + "'");
        } finally {
            bufferMgr.unpin(buf);
        }
    }
}
