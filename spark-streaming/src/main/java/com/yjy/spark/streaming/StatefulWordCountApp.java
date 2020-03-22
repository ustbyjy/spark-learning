package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * mapWithState 比 updateStateByKey 效率更高，建议在生产环境使用
 */
public class StatefulWordCountApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("StatefulWordCountApp");

        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));
        // 如果使用stateful的算子，必须要设置checkpoint
        streamingContext.checkpoint("tmp");

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("vhost1", 29999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.updateStateByKey((values, state) -> {
            Integer newSum = (state.isPresent() ? state.get() : 0) + values.stream().mapToInt(Integer::intValue).sum();

            return Optional.of(newSum);
        });

//        JavaMapWithStateDStream<String, Integer, Integer, Tuple2<String, Integer>> wordCounts = pairs.mapWithState(StateSpec.function(new Function3<String, Optional<Integer>, State<Integer>, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> call(String v1, Optional<Integer> v2, State<Integer> v3) throws Exception {
//                int sum = v2.orElse(0) + Optional.ofNullable(v3.get()).orElse(0);
//                return new Tuple2<>(v1, sum);
//            }
//        }));

        wordCounts.print();

        streamingContext.start();

        streamingContext.awaitTermination();
    }

}
