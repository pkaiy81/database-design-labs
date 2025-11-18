package app.example;

import app.memory.BufferMgr;
import app.memory.LogManager;
import app.storage.BlockId;
import app.storage.FileMgr;
import app.tx.LogCodec;
import app.tx.Tx;
import app.tx.recovery.CheckpointManager;
import app.tx.recovery.RecoveryManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Phase 2: Recovery Manager デモプログラム
 * 
 * <p>
 * このデモは3つのシナリオを実演します:
 * <ol>
 * <li>クラッシュシミュレーション - 未コミットトランザクションのUNDO</li>
 * <li>チェックポイント機能 - アクティブトランザクションの記録</li>
 * <li>リカバリ処理 - システム起動時の自動復旧</li>
 * </ol>
 * 
 * @see RecoveryManager
 * @see CheckpointManager
 */
public class RecoveryDemo {
    private static final Path TEMP_DIR = Path.of("data-demo-recovery");

    public static void main(String[] args) {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║  Phase 2: Recovery Manager Demo                     ║");
        System.out.println("║  Crash Recovery & Checkpoint Demonstration           ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
        System.out.println();

        try {
            // クリーンアップ
            cleanupTempDir();

            // シナリオ1: クラッシュシミュレーション（未コミットトランザクション）
            scenario1_CrashWithUncommittedTransaction();

            System.out.println("\n" + "=".repeat(60) + "\n");

            // クリーンアップ
            cleanupTempDir();

            // シナリオ2: コミット済みトランザクションはリカバリ不要
            scenario2_CommittedTransactionSurvives();

            System.out.println("\n" + "=".repeat(60) + "\n");

            // クリーンアップ
            cleanupTempDir();

            // シナリオ3: チェックポイントからのリカバリ
            scenario3_CheckpointRecovery();

        } catch (Exception e) {
            System.err.println("Demo failed: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 最終クリーンアップ
            cleanupTempDir();
        }

        System.out.println("\n╔══════════════════════════════════════════════════════╗");
        System.out.println("║  Phase 2 Demo Complete!                              ║");
        System.out.println("╚══════════════════════════════════════════════════════╝");
    }

    /**
     * シナリオ1: クラッシュシミュレーション - 未コミットトランザクションのUNDO
     * 
     * <p>
     * ストーリー:
     * <ol>
     * <li>Tx1がアカウントの残高を100 → 999に変更</li>
     * <li>コミット前にシステムクラッシュ</li>
     * <li>リカバリマネージャが起動し、変更を100に戻す（UNDO）</li>
     * </ol>
     */
    private static void scenario1_CrashWithUncommittedTransaction() throws IOException {
        System.out.println("【シナリオ1】クラッシュシミュレーション - 未コミットトランザクション");
        System.out.println();

        Path dataDir = TEMP_DIR.resolve("data");
        Path logDir = TEMP_DIR.resolve("log");
        Files.createDirectories(dataDir);
        Files.createDirectories(logDir);

        FileMgr fm = new FileMgr(dataDir, 400);
        LogManager lm = new LogManager(logDir);
        BufferMgr bm = new BufferMgr(fm, 400, 8);

        BlockId accountBlock = new BlockId("account.dat", 0);

        System.out.println("Phase 1: トランザクション開始 - 残高を変更（コミットしない）");
        System.out.println("-".repeat(60));

        // ファイルを事前に作成
        fm.append(accountBlock.filename());

        // トランザクション開始（コミットしない = クラッシュをシミュレート）
        {
            Tx tx = new Tx(fm, bm, lm, logDir);
            System.out.println("Tx " + tx.id() + " started.");

            var buf = bm.pin(accountBlock);
            try {
                // 初期値を設定
                buf.contents().setInt(0, 100);
                buf.setDirty();
                buf.flushIfDirty();
                System.out.println("  初期残高: 100");

                // UNDOログを記録
                byte[] log = LogCodec.setInt(tx.id(), accountBlock.filename(), accountBlock.number(), 0, 100);
                long lsn = lm.append(log);
                lm.flush(lsn); // ログをフラッシュ
                System.out.println("  UNDOログ記録: SET_INT(tx=" + tx.id() + ", offset=0, oldVal=100)");

                // 新しい値に変更（不正な残高増加をシミュレート）
                buf.contents().setInt(0, 999);
                buf.setDirty();
                buf.flushIfDirty();
                System.out.println("  残高変更: 100 → 999");

            } finally {
                bm.unpin(buf);
            }

            // コミットせずに終了 = クラッシュ
            System.out.println("  ⚠️  システムクラッシュ！（tx.commit() を呼ばずに終了）");
        }

        System.out.println();
        System.out.println("Phase 2: システム再起動 - リカバリ実行");
        System.out.println("-".repeat(60));

        // リカバリマネージャで復旧
        RecoveryManager recoveryMgr = new RecoveryManager(logDir.toFile(), bm);
        recoveryMgr.recover();

        System.out.println();
        System.out.println("Phase 3: リカバリ後の値を確認");
        System.out.println("-".repeat(60));

        // 値を確認
        var buf = bm.pin(accountBlock);
        try {
            int recoveredBalance = buf.contents().getInt(0);
            System.out.println("  リカバリ後の残高: " + recoveredBalance);

            if (recoveredBalance == 100) {
                System.out.println("  ✅ UNDO成功！未コミットの変更が元に戻されました");
            } else {
                System.out.println("  ❌ UNDO失敗: 期待値=100, 実際=" + recoveredBalance);
            }
        } finally {
            bm.unpin(buf);
        }

        lm.close();
    }

    /**
     * シナリオ2: コミット済みトランザクションはリカバリ不要
     * 
     * <p>
     * ストーリー:
     * <ol>
     * <li>Tx2がアカウントの残高を100 → 555に変更</li>
     * <li>正常にコミット</li>
     * <li>システム再起動後も555のまま（リカバリ不要）</li>
     * </ol>
     */
    private static void scenario2_CommittedTransactionSurvives() throws IOException {
        System.out.println("【シナリオ2】コミット済みトランザクション - リカバリ不要");
        System.out.println();

        Path dataDir = TEMP_DIR.resolve("data");
        Path logDir = TEMP_DIR.resolve("log");
        Files.createDirectories(dataDir);
        Files.createDirectories(logDir);

        FileMgr fm = new FileMgr(dataDir, 400);
        LogManager lm = new LogManager(logDir);
        BufferMgr bm = new BufferMgr(fm, 400, 8);

        BlockId accountBlock = new BlockId("account2.dat", 0);

        System.out.println("Phase 1: トランザクション開始 - 残高を変更してコミット");
        System.out.println("-".repeat(60));

        // ファイルを事前に作成
        fm.append(accountBlock.filename());

        {
            Tx tx = new Tx(fm, bm, lm, logDir);
            System.out.println("Tx " + tx.id() + " started.");

            var buf = bm.pin(accountBlock);
            try {
                buf.contents().setInt(0, 100);
                buf.setDirty();
                buf.flushIfDirty();
                System.out.println("  初期残高: 100");

                byte[] log = LogCodec.setInt(tx.id(), accountBlock.filename(), accountBlock.number(), 0, 100);
                long lsn = lm.append(log);
                lm.flush(lsn);
                System.out.println("  UNDOログ記録: SET_INT(tx=" + tx.id() + ", offset=0, oldVal=100)");

                buf.contents().setInt(0, 555);
                buf.setDirty();
                buf.flushIfDirty();
                System.out.println("  残高変更: 100 → 555");

            } finally {
                bm.unpin(buf);
            }

            tx.commit();
            System.out.println("  ✅ コミット成功");
        }

        System.out.println();
        System.out.println("Phase 2: システム再起動 - リカバリ実行");
        System.out.println("-".repeat(60));

        RecoveryManager recoveryMgr = new RecoveryManager(logDir.toFile(), bm);
        recoveryMgr.recover();

        System.out.println();
        System.out.println("Phase 3: リカバリ後の値を確認");
        System.out.println("-".repeat(60));

        var buf = bm.pin(accountBlock);
        try {
            int recoveredBalance = buf.contents().getInt(0);
            System.out.println("  リカバリ後の残高: " + recoveredBalance);

            if (recoveredBalance == 555) {
                System.out.println("  ✅ コミット済みの変更は維持されました");
            } else {
                System.out.println("  ❌ 異常: 期待値=555, 実際=" + recoveredBalance);
            }
        } finally {
            bm.unpin(buf);
        }

        lm.close();
    }

    /**
     * シナリオ3: チェックポイントからのリカバリ
     * 
     * <p>
     * ストーリー:
     * <ol>
     * <li>Tx3, Tx4が実行中</li>
     * <li>チェックポイント実行 → アクティブTxリストを記録</li>
     * <li>Tx3はコミット、Tx4はクラッシュ</li>
     * <li>リカバリ時、チェックポイントのリストからTx4のみUNDO</li>
     * </ol>
     */
    private static void scenario3_CheckpointRecovery() throws IOException {
        System.out.println("【シナリオ3】チェックポイントからのリカバリ");
        System.out.println();

        Path dataDir = TEMP_DIR.resolve("data");
        Path logDir = TEMP_DIR.resolve("log");
        Files.createDirectories(dataDir);
        Files.createDirectories(logDir);

        FileMgr fm = new FileMgr(dataDir, 400);
        LogManager lm = new LogManager(logDir);
        BufferMgr bm = new BufferMgr(fm, 400, 8);

        CheckpointManager checkpointMgr = new CheckpointManager(lm, 5);

        System.out.println("Phase 1: 複数トランザクションの実行");
        System.out.println("-".repeat(60));

        Tx tx3 = new Tx(fm, bm, lm, logDir);
        Tx tx4 = new Tx(fm, bm, lm, logDir);

        System.out.println("Tx " + tx3.id() + " started.");
        System.out.println("Tx " + tx4.id() + " started.");

        checkpointMgr.registerTransaction(tx3.id());
        checkpointMgr.registerTransaction(tx4.id());

        System.out.println();
        System.out.println("Phase 2: チェックポイント実行");
        System.out.println("-".repeat(60));

        checkpointMgr.checkpoint();

        System.out.println();
        System.out.println("Phase 3: Tx3はコミット、Tx4はクラッシュ");
        System.out.println("-".repeat(60));

        tx3.commit();
        System.out.println("  ✅ Tx " + tx3.id() + " committed.");
        checkpointMgr.unregisterTransaction(tx3.id());

        // Tx4はコミットせずに終了（クラッシュ）
        System.out.println("  ⚠️  Tx " + tx4.id() + " crashed without commit!");

        System.out.println();
        System.out.println("Phase 4: システム再起動 - チェックポイントからのリカバリ");
        System.out.println("-".repeat(60));

        RecoveryManager recoveryMgr = new RecoveryManager(logDir.toFile(), bm);
        recoveryMgr.recover();

        System.out.println();
        System.out.println("  ✅ チェックポイントのアクティブTxリストを使用してリカバリ完了");

        lm.close();
    }

    /**
     * 一時ディレクトリのクリーンアップ
     */
    private static void cleanupTempDir() {
        try {
            if (Files.exists(TEMP_DIR)) {
                Files.walk(TEMP_DIR)
                        .sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(java.io.File::delete);
            }
        } catch (IOException e) {
            // クリーンアップ失敗は無視
        }
    }
}
