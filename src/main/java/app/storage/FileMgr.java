package app.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/**
 * OSファイルを「固定長ブロック」単位で扱う最小ファイルマネージャ。
 * - read(block) / write(block) / append(file) / length(file)
 */
public final class FileMgr {
    private final Path dbDir;
    private final int blockSize;

    public FileMgr(Path dbDir, int blockSize) {
        if (blockSize <= 0)
            throw new IllegalArgumentException("blockSize must be > 0");
        this.dbDir = dbDir;
        this.blockSize = blockSize;
        try {
            Files.createDirectories(dbDir);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create db directory: " + dbDir, e);
        }
    }

    public int blockSize() {
        return blockSize;
    }

    private Path path(String filename) {
        return dbDir.resolve(filename);
    }

    /** 指定ブロックを読み込む（不足分はゼロ埋め） */
    public synchronized void read(BlockId blk, Page p) {
        try (FileChannel fc = FileChannel.open(path(blk.filename()),
                StandardOpenOption.READ, StandardOpenOption.CREATE)) {
            fc.position((long) blk.number() * blockSize);
            ByteBuffer buf = ByteBuffer.wrap(p.contents());
            int total = 0;
            while (buf.hasRemaining()) {
                int n = fc.read(buf);
                if (n < 0)
                    break; // EOF
                total += n;
            }
            // 読み込み不足分は ByteBuffer が自動で0のまま
        } catch (IOException e) {
            throw new RuntimeException("read failed: " + blk, e);
        }
    }

    /** 指定ブロックへ書き込む（force(true) でメタデータ含め同期） */
    public synchronized void write(BlockId blk, Page p) {
        try (FileChannel fc = FileChannel.open(path(blk.filename()),
                StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            fc.position((long) blk.number() * blockSize);
            fc.write(ByteBuffer.wrap(p.contents()));
            fc.force(true);
        } catch (IOException e) {
            throw new RuntimeException("write failed: " + blk, e);
        }
    }

    /** ファイル末尾に空ブロックを追加し、その BlockId を返す */
    public synchronized BlockId append(String filename) {
        Path file = path(filename);
        try (FileChannel fc = FileChannel.open(file,
                StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)) {
            int newBlkNum = (int) (fc.size() / blockSize);
            fc.position((long) newBlkNum * blockSize);
            fc.write(ByteBuffer.allocate(blockSize)); // ゼロで1ブロック拡張
            fc.force(true);
            return new BlockId(filename, newBlkNum);
        } catch (IOException e) {
            throw new RuntimeException("append failed: " + filename, e);
        }
    }

    /** ファイルが何ブロック分あるか（0始まりではなく個数） */
    public int length(String filename) {
        try {
            Path p = path(filename);
            long size = Files.exists(p) ? Files.size(p) : 0L;
            return (int) (size / blockSize);
        } catch (IOException e) {
            throw new RuntimeException("length failed: " + filename, e);
        }
    }
}
