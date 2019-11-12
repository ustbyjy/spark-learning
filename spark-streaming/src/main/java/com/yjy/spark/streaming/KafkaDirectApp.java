package com.yjy.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

public class KafkaDirectApp {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("KafkaDirectApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        String topics = "spark_streaming_direct";
        Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));

        Map<String, String> kafkaParams = new HashMap<>();
        String brokers = "vhost1:9092";
        kafkaParams.put("bootstrap.servers", brokers);

        JavaPairInputDStream<String, String> directStream = KafkaUtils.createDirectStream(streamingContext, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

//        JavaPairDStream<String, Integer> wordCounts = directStream
//                .map(tuple2 -> tuple2._2)
//                .flatMap(x -> Arrays.asList(SPACE.split(x)).iterator())
//                .mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<String, Integer>(word, 1))
//                .reduceByKey(Integer::sum);
//        wordCounts.print();

        final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<>();

        directStream.transformToPair(rdd -> {
                    // 向上转换 KafkaRDD -> HasOffsetRanges
                    OffsetRange[] offsets = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                    offsetRanges.set(offsets);
                    return rdd;
                }
        ).map(x -> x._2).foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
                    for (OffsetRange o : offsetRanges.get()) {
                        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                    }
                }
        );

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
