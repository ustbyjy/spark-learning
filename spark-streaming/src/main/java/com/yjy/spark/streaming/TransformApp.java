package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 黑名单过滤
 */
public class TransformApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TransformApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        List<String> blacks = Arrays.asList("zs", "ls");
        JavaPairRDD<String, Boolean> blacksRDD = streamingContext.sparkContext().parallelize(blacks).
                mapToPair((PairFunction<String, String, Boolean>) x -> new Tuple2<String, Boolean>(x, true));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("vhost1", 9999);

        // 使用左外连接，join是内连接
        JavaDStream<String> cleanedDStream = lines.mapToPair((PairFunction<String, String, String>) x -> new Tuple2<String, String>(x.split(",")[1], x))
                .transform(rdd -> rdd.leftOuterJoin(blacksRDD)
                        .filter(x -> !x._2._2.orElse(false))
                        .map(x -> x._2._1)
                );

        cleanedDStream.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
