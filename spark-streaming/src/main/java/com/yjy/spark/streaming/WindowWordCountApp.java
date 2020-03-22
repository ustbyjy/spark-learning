package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 每隔4秒统计前6秒
 */
public class WindowWordCountApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("WindowWordCountApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("vhost1", 29999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKeyAndWindow(Integer::sum, Duration.apply(6 * 1000), Duration.apply(2 * 1000));

        wordCounts.print();

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
