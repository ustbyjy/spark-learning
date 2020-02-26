package com.yjy.spark.core;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;

public class ReduceJoinApp {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        sparkSession = SparkSession.builder()
                .appName("ReduceJoinApp")
                .master("local[2]")
                .getOrCreate();
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> table1 = sparkContext.parallelize(Arrays.asList("k1,v11", "k2,v12", "k3,v13"));
        JavaRDD<String> table2 = sparkContext.parallelize(Arrays.asList("k1,v21", "k4,v24", "k3,v23"));

        JavaPairRDD<String, String> pairs1 = table1.mapToPair((PairFunction<String, String, String>) s -> {
            int pos = s.indexOf(",");
            return new Tuple2<>(s.substring(0, pos), s.substring(pos + 1));
        });
        JavaPairRDD<String, String> pairs2 = table2.mapToPair((PairFunction<String, String, String>) s -> {
            int pos = s.indexOf(",");
            return new Tuple2<>(s.substring(0, pos), s.substring(pos + 1));
        });

        JavaPairRDD<String, Tuple2<String, String>> join = pairs1.join(pairs2);
        join.saveAsTextFile("/tmp/spark/ReduceJoinApp-" + System.currentTimeMillis());

        sparkSession.stop();
    }

}
