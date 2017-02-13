package com.zexin.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.File;

/**
 * Created by bianzexin on 17/2/13.
 */
public class Searcher {
    public static void search(String indexDir, String q) throws Exception {
        Directory dir = FSDirectory.open(new File(indexDir));
        IndexReader reader = DirectoryReader.open(dir);
        IndexSearcher indexSearcher = new IndexSearcher(reader);
        Analyzer analyzer = new StandardAnalyzer();
        QueryParser parser = new QueryParser("fileName", analyzer);
        Query query = parser.parse(q);
        TopDocs hits = indexSearcher.search(query, 10);
        System.out.println("匹配 " + q + "查询到" + hits.totalHits + "个记录");
        for (ScoreDoc scoreDoc : hits.scoreDocs) {
            Document doc = indexSearcher.doc(scoreDoc.doc);
            System.out.println(doc.get("fileName"));//打印Document的fileName属性
        }
        reader.close();
    }

    public static void main(String[] args) {
        String indexDir = "/Users/bianzexin/Documents/LuceneTest/";
        String q = "Lucene";
        try {
            search(indexDir, q);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
