package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class StatefulWordCountApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCountApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        // 如果使用stateful的算子，必须要设置checkpoint
        streamingContext.checkpoint("tmp");

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("vhost1", 9999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<String, Integer>(word, 1));

        Function2<List<Integer>, Optional<Integer>, Optional<Integer>> updateFunction =
                (values, state) -> {
                    Integer newSum = (state.isPresent() ? state.get() : 0) + values.stream().mapToInt(Integer::intValue).sum();

                    return Optional.of(newSum);
                };

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey(updateFunction);

        wordCounts.print();

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
