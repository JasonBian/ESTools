package com.zexin.tools;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Bits;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by bianzexin on 16/12/19.
 */
public class LuceneIndex {

    private static int QUEUE_SIZE = 1000000;
    private static int FILE_LINE = 10000;

    private static AtomicLong totalCount = new AtomicLong(0);
    private static AtomicLong readCount = new AtomicLong(0);
    private static AtomicLong threadCount = new AtomicLong(0);
    private static AtomicLong writeCount = new AtomicLong(0);
    private static AtomicLong readThreadCount = new AtomicLong(5);
    private static LinkedBlockingQueue<String> validValueQueue = new LinkedBlockingQueue<String>(500000);

    public static MBeanThreadPoolExecutor te = new MBeanThreadPoolExecutor(LuceneIndex.class.getPackage().getName()
            + ":name=" + LuceneIndex.class.getSimpleName(), Runtime.getRuntime().availableProcessors() * 2, Runtime
            .getRuntime().availableProcessors() * 2, 10L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>(
            QUEUE_SIZE), new ThreadFactory() {
        final AtomicLong threadNumber = new AtomicLong(1);

        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, "bulkinsert-thread-" + threadNumber.getAndIncrement());
            t.setDaemon(true);
            t.setPriority(Thread.MAX_PRIORITY);
            return t;
        }
    }, new ThreadPoolExecutor.CallerRunsPolicy());


    public static void main(String[] args) throws IOException, InterruptedException {
        long beginTime = System.currentTimeMillis();

        File[] files = new File(System.getProperty("luceneIndexFilePath")).listFiles();
        if (files.length > 0) {
            for (File file : files) {
                final int shardNum = Integer.parseInt(file.getName());
                Thread t = new Thread(new ReadIndexWorker(shardNum));
                t.setDaemon(true);
                t.setPriority(Thread.MIN_PRIORITY);
                t.start();
            }
        }
        new Thread(new WriteFileWorker(System.getProperty("dstFilePath"))).start();

        while (true) {
            Thread.sleep(5000);
        }
    }

    public static class WriteFileWorker implements Runnable {
        String fileDir = null;

        public WriteFileWorker(String fileDir) {
            this.fileDir = fileDir;
        }

        private void changeFileName(String fileName) {
            File file = new File(fileName);
            file.renameTo(new File(fileName.substring(0, (fileName.length() - 3))));
        }

        @Override
        public void run() {
            BufferedRandomFileWriter writer = null;
            String fileName = null;
            while (true) {
                try {
                    if (writeCount.get() % FILE_LINE == 0) {
                        if (writer != null) {
                            writer.close();
                            changeFileName(fileName);
                        }
                        String sep = File.separator;
                        if (!fileDir.endsWith(sep)) {
                            fileDir = fileDir + sep;
                        }

                        File fileRoot = new File(fileDir);
                        if (!fileRoot.exists()) {
                            fileRoot.mkdir();
                        }
                        int fileNum = (int) (writeCount.get() / FILE_LINE);
                        if (fileDir.endsWith(sep)) {
                            fileName = fileDir + fileNum + ".loging";
                        } else {
                            fileName = fileDir + sep + fileNum + ".loging";
                        }
                        writer = new BufferedRandomFileWriter(fileName, 512 * 1024);
                    }
                    String data = validValueQueue.poll(Long.valueOf(System.getProperty("validValueResponseTime")), TimeUnit.SECONDS);
                    if (StringUtils.isBlank(data) && readThreadCount.get() == 0) {
                        if (writer != null) {
                            writer.close();
                            changeFileName(fileName);
                        }
                        break;
                    }
                    if (StringUtils.isNotBlank(data)) {
                        writeCount.incrementAndGet();
                        writer.write(data.getBytes());
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            if (writer != null) {
                writer.close();
                changeFileName(fileName);
            }

        }

    }

    public static class CheckUserWorker implements Runnable {
        private UserExtInfos user;

        public CheckUserWorker(UserExtInfos user) {
            this.user = user;
        }

        @Override
        public void run() {
                try {
                    StringBuilder sb = new StringBuilder();
                    sb.append(StringUtils.isEmpty(user.getUsername()) ? "\\N" : user.getUsername()).append("|");
                    sb.append(StringUtils.isEmpty(user.getSex()) ? "\\N" : user.getSex()).append("|");
                    sb.append(StringUtils.isEmpty(user.getAddress()) ? "\\N" : user.getAddress()).append("|");
                    sb.append("\r\n");
                    validValueQueue.put(sb.toString());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }


    public static class ReadIndexWorker implements Runnable {
        private int shardNum;

        public ReadIndexWorker(int shardNum) {
            this.shardNum = shardNum;
        }

        public void run() {
            try {
                String filePath = System.getProperty("LuceneIndexFilePath") + shardNum + File.separator + "index";
                Directory dir = FSDirectory.open(new File(filePath));
                IndexReader ir = DirectoryReader.open(dir);
                int num = ir.maxDoc();
                totalCount.addAndGet(num);
                Bits liveDocs = MultiFields.getLiveDocs(ir);
                ObjectMapper ob = new ObjectMapper();
                for (int j = 0; j < num; j++) {
                    try {
                        if (liveDocs == null || liveDocs.get(j)) {
                            Document doc = ir.document(j);
                            String jsonString = new String(doc.getField("_source").binaryValue().bytes);
                            try {
                                UserExtInfos user = ob.readValue(jsonString, UserExtInfos.class);
                                while (te.getQueueSize4Jmx() >= (QUEUE_SIZE - 100000)) {
                                    Thread.sleep(1000);
                                    System.out.print("queue size :" + te.getQueueSize4Jmx());
                                }
                                te.execute(new CheckUserWorker(user));
                            } catch (Exception e) {
                                System.out.print("Read Index doc error" + e);
                            }
                        } else {

                        }
                    } finally {
                        readCount.incrementAndGet();
                    }
                }
                ir.close();
                readThreadCount.addAndGet(-1);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                threadCount.incrementAndGet();
            }
        }

    }
}
