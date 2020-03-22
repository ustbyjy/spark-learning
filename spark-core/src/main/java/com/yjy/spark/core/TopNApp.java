package com.yjy.spark.core;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TopNApp {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        sparkSession = SparkSession.builder()
                .appName("TopNApp")
                .master("local[2]")
                .getOrCreate();
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        String path = TopNApp.class.getClassLoader().getResource("") + "hello.txt";
        List<Tuple2<Integer, String>> list = sparkContext.textFile(path)
                .flatMap((FlatMapFunction<String, String>) s -> Arrays.stream(s.split(",")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) s -> new Tuple2<>(s, 1))
                .reduceByKey((Function2<Integer, Integer, Integer>) Integer::sum)
                .mapToPair((PairFunction<Tuple2<String, Integer>, Integer, String>) t -> new Tuple2<>(t._2, t._1))
                .sortByKey(false)
                .take(3);

        list.forEach(t -> System.out.println(t._1 + ": " + t._2));

        sparkSession.stop();
    }

}
