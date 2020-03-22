package com.yjy.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class RDDTransformationsApp {

    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("RDDTransformationsApp")
                .master("local[2]")
                .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

//        testMap();
//        testFlatMap();
        testFlatMapFilter();
//        testMapPartitions();
//        testReduceByKey();
//        testGroupByKey();
//        testAggregateByKey();
//        testDistinct();

        sparkSession.stop();
    }

    private static void testMap() {
        List<Integer> data = Arrays.asList(1, 2, 3);
        JavaRDD<Integer> rdd = sparkContext.parallelize(data);

        List<Integer> result = rdd.map((Function<Integer, Integer>) v -> v * v).collect();

        System.out.println(result);
    }

    private static void testFlatMap() {
        List<String> data = Arrays.asList("1,2,3", "4,5,6", "7,8,9");
        JavaRDD<String> rdd = sparkContext.parallelize(data);

        List<Integer> result = rdd.flatMap((FlatMapFunction<String, Integer>) s -> Arrays.stream(s.split(",")).mapToInt(Integer::valueOf).iterator()).collect();

        System.out.println(result);
    }

    private static void testFlatMapFilter() {
        List<String> data = Arrays.asList("1", "2", "1", "1", "4", "3", "3", "1", "5", "6");
        List<Tuple2<String, String>> list = sparkContext.parallelize(data)
                .map((Function<String, Tuple2<String, String>>) v1 -> new Tuple2<>(v1, v1 + "asd"))
                .flatMap((FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>) tuple2 -> {
                    if (tuple2._2.startsWith("3")) {
                        return Collections.singleton(new Tuple2<>(tuple2._1, tuple2._2 + "--b")).iterator();
                    } else {
                        return Collections.EMPTY_LIST.iterator();
                    }
                }).collect();
        list.forEach(System.out::println);
    }

    private static void testMapPartitions() {
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6);
        JavaRDD<Integer> rdd = sparkContext.parallelize(data);

        List<Integer> result = rdd.mapPartitions((FlatMapFunction<Iterator<Integer>, Integer>) iterator -> {
            List<Integer> list = new ArrayList<>();
            while (iterator.hasNext()) {
                int v = iterator.next();
                list.add(v * v);
            }
            return list.iterator();
        }).collect();

        System.out.println(result);
    }

    private static void testReduceByKey() {
        List<String> data = Arrays.asList("hello world spark hadoop", "java hadoop hive", "hadoop spark flink");
        JavaRDD<String> rdd = sparkContext.parallelize(data);
        List<Tuple2<String, Integer>> result = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .collect();

        for (Tuple2<String, Integer> tuple2 : result) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }
    }

    private static void testGroupByKey() {
        List<String> data = Arrays.asList("hello world spark hadoop", "java hadoop hive", "hadoop spark flink");
        JavaRDD<String> rdd = sparkContext.parallelize(data);
        List<Tuple2<String, Integer>> result = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .groupByKey().map((Function<Tuple2<String, Iterable<Integer>>, Tuple2<String, Integer>>) v -> {
                    Iterator<Integer> iterator = v._2.iterator();
                    int sum = 0;
                    while (iterator.hasNext()) {
                        sum += iterator.next();
                    }
                    return new Tuple2<>(v._1, sum);
                })
                .collect();

        for (Tuple2<String, Integer> tuple2 : result) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }
    }

    private static void testAggregateByKey() {
        List<String> data = Arrays.asList("hello world spark hadoop", "java hadoop hive", "hadoop spark flink");
        JavaRDD<String> rdd = sparkContext.parallelize(data);
        List<Tuple2<String, Integer>> result = rdd.flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .aggregateByKey(0, (Function2<Integer, Integer, Integer>) Integer::sum, (Function2<Integer, Integer, Integer>) Integer::sum)
                .collect();

        for (Tuple2<String, Integer> tuple2 : result) {
            System.out.println(tuple2._1 + ": " + tuple2._2);
        }
    }

    private static void testDistinct() {
        List<Integer> data1 = Arrays.asList(1, 2, 3);
        List<Integer> data2 = Arrays.asList(2, 3, 4);

        JavaRDD<Integer> rdd1 = sparkContext.parallelize(data1);
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(data2);

        System.out.println(rdd1.union(rdd2).distinct().collect());
    }

}
