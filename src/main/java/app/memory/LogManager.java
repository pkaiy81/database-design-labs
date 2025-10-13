// src/main/java/app/memory/LogManager.java
package app.memory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

public final class LogManager implements AutoCloseable {
    private final Path logPath;
    private FileChannel fc;

    public LogManager(Path dir) {
        try {
            Files.createDirectories(dir);
            this.logPath = dir.resolve("simpledb.log");
            this.fc = FileChannel.open(logPath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** レコードを末尾に追記し、その LSN（=ファイルオフセット）を返す */
    public synchronized long append(byte[] record) {
        try {
            long lsn = fc.size();
            fc.position(lsn);
            // 先頭に長さを書いてから本体を書き込む（可変長対応）
            ByteBuffer len = ByteBuffer.allocate(Integer.BYTES);
            len.putInt(record.length).flip();
            fc.write(len);
            fc.write(ByteBuffer.wrap(record));
            return lsn;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /** 指定 LSN までフラッシュ（ここでは単純に force） */
    public synchronized void flush(long uptoLsn) {
        try {
            fc.force(true); // メタデータ込みで同期
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized void close() {
        try {
            if (fc != null)
                fc.close();
        } catch (IOException ignore) {
        }
    }
}
