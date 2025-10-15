package app.index.btree;

import app.storage.FileMgr;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

final class TestIO {
    static final int BLOCK_SIZE = 4096;

    static class Env implements AutoCloseable {
        final Path dir;
        final FileMgr fm;

        Env() throws IOException {
            this.dir = Files.createTempDirectory("btree-test-");
            this.fm = new FileMgr(dir, BLOCK_SIZE);
        }

        String dataFile(String base) {
            return base + ".tbl";
        }

        String indexFile(String base) {
            return "idx_" + base;
        }

        @Override
        public void close() throws Exception {
            try {
                // fm.close();
            } catch (Throwable ignore) {
            }
            // テスト終わりで消す（Windowsでロックが残る場合は手動削除に切り替え）
            try (var s = Files.walk(dir)) {
                s.sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try {
                        Files.deleteIfExists(p);
                    } catch (IOException ignore) {
                    }
                });
            }
        }
    }
}
