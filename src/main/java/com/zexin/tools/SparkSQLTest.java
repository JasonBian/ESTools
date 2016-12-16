package com.zexin.tools;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.List;

/**
 * Created by bianzexin on 16/12/16.
 */
public class SparkSQLTest {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("elastic-spark sql test").setMaster("local").set("es.nodes","localhost:9200");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        SQLContext sql = new SQLContext(jsc);
        DataFrame df = JavaEsSparkSQL.esDF(sql, "userindex/R6Z7Ews31c8O4BTidZaWyA");
        DataFrame playlist = df.filter(df.col("province").equalTo("北京"));
        List<Row> rowList = playlist.collectAsList();
        if (rowList != null && rowList.size() > 0) {
            for (Row row : rowList) {
                String rowString = row.toString();
                System.out.println(rowString);
            }
        }
        System.out.println(df);
    }
}