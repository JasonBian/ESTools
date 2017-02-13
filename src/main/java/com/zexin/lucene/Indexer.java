package com.zexin.lucene;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import java.io.File;
import java.io.FileReader;

/**
 * Created by bianzexin on 17/2/13.
 */
public class Indexer {
    public IndexWriter writer;

    public Indexer(String indexDir) throws Exception {
        Analyzer analyzer = new StandardAnalyzer();
        IndexWriterConfig writerConfig = new IndexWriterConfig(Version.LUCENE_4_10_4, analyzer);
        Directory directory = FSDirectory.open(new File(indexDir));
        writer = new IndexWriter(directory, writerConfig);
    }

    public void close() throws Exception {
        writer.close();
    }

    public int index(String dataDir) throws Exception {
        File[] files = new File(dataDir).listFiles();
        for (File file : files) {
            indexFile(file);
        }
        return writer.numDocs();
    }

    public void indexFile(File file) throws Exception {
        System.out.println("索引文件:" + file.getCanonicalPath());
        Document document = getDocument(file);
        writer.addDocument(document);
    }

    public Document getDocument(File file) throws Exception {
        Document document = new Document();
        document.add(new TextField("context", new FileReader(file)));
        document.add(new TextField("fileName", file.getName(), Field.Store.YES));
        document.add(new TextField("filePath", file.getCanonicalPath(), Field.Store.YES));
        return document;
    }

    public static void main(String[] args) {
        String indexDir = "/Users/bianzexin/Documents/LuceneTest/";
        String dataDir = "/Users/bianzexin/Documents/work/Lucene";
        Indexer indexer = null;
        int indexSum = 0;
        try  {
            indexer = new Indexer(indexDir);
            indexSum = indexer.index(dataDir);
            System.out.println("完成" + indexSum + "个文件的索引");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                indexer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
