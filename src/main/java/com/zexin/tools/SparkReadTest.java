package com.zexin.tools;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by bianzexin on 16/12/16.
 */
public class SparkReadTest {
    public static void main(String[] args) {
        String queryString = "{\"query\":{\"bool\":{\"must\":[{\"term\":{\"phoneType\":\"ANDROID\"}}],\"must_not\":[],\"should\":[]}},\"from\":0,\"size\":10,\"sort\":[]}";
        SparkConf conf = new SparkConf()
                .setAppName("Spark Read Elasticsearch Test").setMaster("local")
                .set("es.nodes", "localhost")
                .set("es.query", queryString);
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "userindex/951");
        if (esRDD != null) {
            Map<String, Map<String, Object>> map = esRDD.collectAsMap();
            Set keys = map.keySet();
            if (keys != null) {
                Iterator iterator = keys.iterator();
                while (iterator.hasNext()) {
                    Object key = iterator.next();
                    System.out.println(key.toString());
                    Map<String, Object> value = map.get(key);
                    System.out.println(value.toString());
                }
            }
            System.out.print("spark Read Elasticsearch success, count=" + esRDD.count());
        }
    }
}

