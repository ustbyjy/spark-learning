package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Spark Streaming整合Spark SQL完成词频统计
 */
public class SqlNetworkWordCountApp {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("SqlNetworkWordCountApp").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = ssc.socketTextStream("vhost1", 9999, StorageLevels.MEMORY_AND_DISK_SER);
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());

        words.foreachRDD((VoidFunction2<JavaRDD<String>, Time>) (rdd, time) -> {
            SparkSession spark = SparkSessionSingleton.getInstance(rdd.context().getConf());

            JavaRDD<Record> rowRDD = rdd.map((Function<String, Record>) word -> {
                Record record = new Record();
                record.setWord(word);
                return record;
            });
            Dataset<Row> wordsDataFrame = spark.createDataFrame(rowRDD, Record.class);

            // Creates a temporary view using the DataFrame
            wordsDataFrame.createOrReplaceTempView("words");

            // Do word count on table using SQL and print it
            Dataset<Row> wordCountsDataFrame = spark.sql("select word, count(*) as total from words group by word");
            System.out.println("========= " + time + "=========");
            wordCountsDataFrame.show();
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
