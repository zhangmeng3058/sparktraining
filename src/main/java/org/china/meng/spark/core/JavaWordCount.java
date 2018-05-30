package org.china.meng.spark.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

public class JavaWordCount {
    public static void main(String[] args) throws IOException {
        String masterUrl = "local[1]";
        String inputFile = "/data/text/words";
        String outputFile = "/tmp/meng/output";

        if (args.length > 0) {
            masterUrl = args[0];
        } else if (args.length > 2) {
            inputFile = args[1];
            outputFile = args[2];
        }

        SparkConf conf = new
                SparkConf().setMaster(masterUrl).setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> input = sc.textFile(inputFile);
//        input.cache();

        JavaRDD<String> words = input.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1.length() > 1;
            }
        });

        JavaPairRDD<String, Integer> counts = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).reduceByKey(
                new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {
                        return v1 + v2;
                    }
                }
        );

        Path outputPath = new Path(outputFile);
        FileSystem fs = outputPath.getFileSystem(new HdfsConfiguration());
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        counts.saveAsTextFile(outputFile);

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(String.format("%s - %d ", stringIntegerTuple2._1(), stringIntegerTuple2._2()));
            }
        });

    }
}
