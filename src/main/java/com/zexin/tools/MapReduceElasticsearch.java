package com.zexin.tools;

/**
 * mapreduce for elasticsearch
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;


public class MapReduceElasticsearch {

    static class EsMapper extends Mapper<Object, Object, Text, Object> {
        @Override
        protected void map(Object key, Object value, Context context) throws IOException, InterruptedException {
            Text docId = (Text) key;
            MapWritable doc = (MapWritable) value;
            context.write(docId, doc);
        }
    }

    static class EsReduce extends Reducer<Text, MapWritable, Text, String> {
        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            String cell = "";
            String provice = "";
            for (MapWritable value : values) {
                Iterable<Writable> keys = value.keySet();
                for (Writable k : keys) {
                    if (k.toString().equals("cell")) {
                        cell = value.get(k).toString();
                    }
                    if (k.toString().equals("province")) {
                        provice = value.get(k).toString();
                    }
                }
            }
            context.write(key, cell + "|" + provice);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.setBoolean("mapred.map.tasks.speculative.execution", false);
        conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
        conf.set("es.nodes", "192.168.10.29:9200,192.168.10.20:9200");
        conf.set("es.resource", "userindex/951");
        conf.set("es.query", "{\"query\":{\"term\":{\"phoneType\":\"ANDROID\"}}}");
        Job job = Job.getInstance(conf, "mapreduce-elasticsearch");
        job.setMapperClass(EsMapper.class);
        job.setReducerClass(EsReduce.class);
        job.setInputFormatClass(EsInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(String.class);
        FileOutputFormat.setOutputPath(job, new Path("/Users/bianzexin/Downloads/esout"));
        job.waitForCompletion(true);
    }

}

