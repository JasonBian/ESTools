package com.zexin.tools;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by bianzexin on 16/12/19.
 */
public class BufferedRandomFileWriter {
    private File file;
    private RandomAccessFile randomAccessFile;
    private FileChannel fileChannel;
    private ByteBuffer byteBuffer;

    public BufferedRandomFileWriter(String fileName, int bufsize) {
        try {
            this.file = new File(fileName);
            File parent = this.file.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            if (!this.file.exists()) {
                this.file.createNewFile();
            }
            this.randomAccessFile = new RandomAccessFile(this.file, "rw");
            this.fileChannel = this.randomAccessFile.getChannel();
            this.byteBuffer = ByteBuffer.allocate(bufsize);
            this.byteBuffer.clear();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void write(byte[] data) {
        int offset = 0;
        do {
            int remaining = this.byteBuffer.remaining();
            int length = Math.min(remaining, data.length - offset);
            this.byteBuffer.put(data, offset, length);
            offset += length;
            if (!this.byteBuffer.hasRemaining())
            {
                flush();
            }
        }
        while (offset < data.length);
    }

    public void write(String data) {
        write(data.getBytes());
    }
    public synchronized void close() {
        try {
            this.byteBuffer.flip();
            this.fileChannel.write(this.byteBuffer);

            this.fileChannel.close();
            this.randomAccessFile.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public synchronized void flush() {
        try {
            this.byteBuffer.flip();
            if (this.fileChannel.isOpen()) {
                this.fileChannel.write(this.byteBuffer);
            }
            this.byteBuffer.compact();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public File getFile() {
        return this.file;
    }

    public static void main(String[] args) {
        BufferedRandomFileWriter w = new BufferedRandomFileWriter("D:\\aa.log", 153600);
        long s = System.nanoTime();
        for (int i = 0; i < 1000000; i++) {
            w.write(("-------------------------aabbccddfsljafksajfkdsajfkja----------------------==============================33333333333333333333=============" + i).getBytes());

            if (i % 10000 == 0) {
                System.out.println("i=" + i);
            }
        }
        long d = System.nanoTime() - s;
        System.out.println("Spent time=" + d / 1000000L + "毫秒");
        w.close();
    }
}
