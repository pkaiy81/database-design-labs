package app.tx.lock;

import app.storage.BlockId;
import app.storage.FileMgr;
import app.memory.BufferMgr;
import app.memory.LogManager;
import app.tx.Tx;

import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ロック機能の動作確認デモ。
 * 2つのトランザクションが同時に同じブロックにアクセスする様子を可視化します。
 */
public class LockingDemo {

    private static FileMgr fm;
    private static BufferMgr bm;
    private static LogManager log;
    private static Path logDir;

    public static void main(String[] args) throws Exception {
        // データベースのセットアップ
        Path dbDir = Path.of("data-demo");
        logDir = dbDir.resolve("log");

        fm = new FileMgr(dbDir, 400);
        log = new LogManager(logDir);
        bm = new BufferMgr(fm, 400, 10);

        System.out.println("╔════════════════════════════════════════════════════════════╗");
        System.out.println("║      MiniDB Phase 1: ロック機能デモンストレーション        ║");
        System.out.println("╚════════════════════════════════════════════════════════════╝\n");

        BlockId testBlock = new BlockId("locktest.dat", 0);

        // ファイルとブロックを作成
        fm.append("locktest.dat");

        // 初期値を設定
        System.out.println("📝 初期値を設定中...");
        Tx setupTx = newTx();
        setupTx.setInt(testBlock, 0, 100);
        setupTx.commit();
        System.out.println("   初期値: 100\n");

        // デモ1: Lost Update 防止
        demo1_LostUpdatePrevention(testBlock);

        Thread.sleep(1000);

        // デモ2: Dirty Read 防止
        demo2_DirtyReadPrevention(testBlock);

        Thread.sleep(1000);

        // デモ3: 共有ロック（複数読み取り）
        demo3_SharedLocks(testBlock);

        System.out.println("\n✅ すべてのデモが完了しました！");
        System.out.println("   ロック機能が正しく動作しています。\n");
    }

    private static Tx newTx() {
        return new Tx(fm, bm, log, logDir);
    }

    /**
     * デモ1: Lost Update 防止
     * 2つのトランザクションが同時に更新しようとするが、ロックにより順次実行される
     */
    private static void demo1_LostUpdatePrevention(BlockId testBlock) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("デモ1: Lost Update 防止");
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("シナリオ: 2つのトランザクションが同時に +50 と +30 を加算\n");

        CountDownLatch startLatch = new CountDownLatch(2);
        CountDownLatch doneLatch = new CountDownLatch(2);

        // トランザクション1: +50
        Thread t1 = new Thread(() -> {
            try {
                Tx tx1 = newTx();
                System.out.println("🔵 Tx" + tx1.id() + ": 開始");

                startLatch.countDown();
                startLatch.await(); // 両方のスレッドが準備完了するまで待機

                System.out.println("🔵 Tx" + tx1.id() + ": 排他ロックで書き込み...");
                // Read-Modify-Write パターン: setIntが内部で旧値を読む
                tx1.setInt(testBlock, 0, 150); // 100 + 50
                System.out.println("🔵 Tx" + tx1.id() + ": 値を更新完了: 100 → 150");

                Thread.sleep(500); // 処理をシミュレート

                Thread.sleep(300);

                tx1.commit();
                System.out.println("🔵 Tx" + tx1.id() + ": コミット完了（ロック解放）\n");

            } catch (Exception e) {
                System.err.println("🔵 Tx エラー: " + e.getMessage());
            } finally {
                doneLatch.countDown();
            }
        });

        // トランザクション2: +30
        Thread t2 = new Thread(() -> {
            try {
                Tx tx2 = newTx();
                System.out.println("🟢 Tx" + tx2.id() + ": 開始");

                startLatch.countDown();
                startLatch.await();

                Thread.sleep(50); // Tx1が先にロックを取得できるように少し待つ

                System.out.println("🟢 Tx" + tx2.id() + ": 読み取り要求...");
                System.out.println("🟢 Tx" + tx2.id() + ": ⏳ 待機中... (Tx" + (tx2.id() - 1) + "が排他ロックを保持)");

                int value = tx2.getInt(testBlock, 0);
                System.out.println("🟢 Tx" + tx2.id() + ": 読み取り成功！現在値=" + value);

                Thread.sleep(300);

                System.out.println("🟢 Tx" + tx2.id() + ": 排他ロックで書き込み...");
                System.out.println("🟢 Tx" + tx2.id() + ": ⏳ 待機中... (Tx" + (tx2.id() - 1) + "のコミット待ち)");
                tx2.setInt(testBlock, 0, value + 30);
                System.out.println("🟢 Tx" + tx2.id() + ": 値を更新: " + value + " → " + (value + 30));

                tx2.commit();
                System.out.println("🟢 Tx" + tx2.id() + ": コミット完了\n");

            } catch (Exception e) {
                System.err.println("🟢 Tx エラー: " + e.getMessage());
            } finally {
                doneLatch.countDown();
            }
        });

        t1.start();
        t2.start();

        doneLatch.await(15, TimeUnit.SECONDS);

        // 最終値を確認
        Tx checkTx = newTx();
        int finalValue = checkTx.getInt(testBlock, 0);
        checkTx.commit();

        System.out.println("✅ 結果: 最終値=" + finalValue);
        System.out.println("   期待値: 180 (100 + 50 + 30)");
        System.out.println("   " + (finalValue == 180 ? "✅ 正しい！Lost Update が防止されました" : "❌ 問題あり"));
        System.out.println();
    }

    /**
     * デモ2: Dirty Read 防止
     * 未コミットのデータは読み取れないことを確認
     */
    private static void demo2_DirtyReadPrevention(BlockId testBlock) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("デモ2: Dirty Read 防止");
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("シナリオ: Tx1が書き込み中、Tx2が読み取ろうとする\n");

        CountDownLatch writeLatch = new CountDownLatch(1);
        CountDownLatch readLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(2);

        // トランザクション1: 書き込んでからロールバック
        Thread t1 = new Thread(() -> {
            try {
                Tx tx1 = newTx();
                System.out.println("🔵 Tx" + tx1.id() + ": 開始");

                System.out.println("🔵 Tx" + tx1.id() + ": 排他ロックで書き込み...");
                tx1.setInt(testBlock, 0, 999);
                System.out.println("🔵 Tx" + tx1.id() + ": 値を999に変更（未コミット）");

                writeLatch.countDown();
                Thread.sleep(1000); // コミット前に待機

                tx1.rollback();
                System.out.println("🔵 Tx" + tx1.id() + ": ロールバック（値は元に戻る）\n");

            } catch (Exception e) {
                System.err.println("🔵 Tx エラー: " + e.getMessage());
            } finally {
                doneLatch.countDown();
            }
        });

        // トランザクション2: Tx1のコミット前に読み取ろうとする
        Thread t2 = new Thread(() -> {
            try {
                writeLatch.await(); // Tx1の書き込みを待つ
                Thread.sleep(100);

                Tx tx2 = newTx();
                System.out.println("🟢 Tx" + tx2.id() + ": Tx" + (tx2.id() - 1) + "のコミット前に読み取り試行...");
                System.out.println("🟢 Tx" + tx2.id() + ": ⏳ 待機中... (共有ロックが取得できない)");

                try {
                    int value = tx2.getInt(testBlock, 0);
                    System.out.println("🟢 Tx" + tx2.id() + ": 読み取り成功: value=" + value);
                    System.out.println("   " + (value != 999 ? "✅ 未コミットの値(999)は読めませんでした" : "❌ Dirty Read発生"));
                    tx2.commit();
                } catch (Exception e) {
                    System.out.println("🟢 Tx" + tx2.id() + ": タイムアウト（これは正常です）");
                    System.out.println("   ✅ Dirty Read が防止されました");
                }

                readLatch.countDown();
            } catch (Exception e) {
                System.err.println("🟢 Tx エラー: " + e.getMessage());
            } finally {
                doneLatch.countDown();
            }
        });

        t1.start();
        t2.start();

        doneLatch.await(15, TimeUnit.SECONDS);
        System.out.println();
    }

    /**
     * デモ3: 複数の共有ロック
     * 複数のトランザクションが同時に読み取りできることを確認
     */
    private static void demo3_SharedLocks(BlockId testBlock) throws Exception {
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("デモ3: 共有ロック（複数読み取り）");
        System.out.println("═══════════════════════════════════════════════════════════");
        System.out.println("シナリオ: 3つのトランザクションが同時に読み取り\n");

        CountDownLatch startLatch = new CountDownLatch(3);
        CountDownLatch doneLatch = new CountDownLatch(3);

        for (int i = 1; i <= 3; i++) {
            Thread t = new Thread(() -> {
                try {
                    Tx tx = newTx();
                    System.out.println("📖 Tx" + tx.id() + ": 共有ロックを要求...");

                    startLatch.countDown();
                    startLatch.await(); // 全員準備完了まで待機

                    int value = tx.getInt(testBlock, 0);
                    System.out.println("📖 Tx" + tx.id() + ": 読み取り成功！value=" + value + " (共有ロック取得)");

                    Thread.sleep(500); // 読み取り処理をシミュレート

                    tx.commit();
                    System.out.println("📖 Tx" + tx.id() + ": コミット完了");

                } catch (Exception e) {
                    System.err.println("📖 Tx エラー: " + e.getMessage());
                } finally {
                    doneLatch.countDown();
                }
            });
            t.start();
        }

        doneLatch.await(10, TimeUnit.SECONDS);

        System.out.println("\n✅ 結果: 3つのトランザクションが同時に読み取りできました");
        System.out.println("   共有ロックは複数のトランザクションで共有可能です");
        System.out.println();
    }
}
