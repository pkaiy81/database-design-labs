package app.tx;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

import java.util.ArrayList;
import java.util.List;

/** ログファイルを先頭から全件読み出して byte[] のリストにして返す簡易リーダ */
public final class LogReader {
    private final Path logPath;

    public LogReader(Path dir) {
        this.logPath = dir.resolve("simpledb.log");
    }

    public List<byte[]> readAll() {
        var list = new ArrayList<byte[]>();
        if (!Files.exists(logPath))
            return list;
        try (FileChannel fc = FileChannel.open(logPath, StandardOpenOption.READ)) {
            long size = fc.size();
            long pos = 0;
            while (pos < size) {
                // 先頭の長さ（int）を読む
                var lenBuf = ByteBuffer.allocate(Integer.BYTES);
                fc.position(pos);
                if (fc.read(lenBuf) != Integer.BYTES)
                    break;
                lenBuf.flip();
                int len = lenBuf.getInt();
                // 本体
                var recBuf = ByteBuffer.allocate(len);
                if (fc.read(recBuf) != len)
                    break;
                list.add(recBuf.array());
                pos += Integer.BYTES + len;
            }
            return list;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
