package com.yjy.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class MapJoinApp {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        sparkSession = SparkSession.builder()
                .appName("MapJoinApp")
                .master("local[2]")
                .getOrCreate();
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        JavaRDD<String> table1 = sparkContext.parallelize(Arrays.asList("k1,v11", "k2,v12", "k3,v13"));
        JavaRDD<String> table2 = sparkContext.parallelize(Arrays.asList("k1,v21", "k4,v24", "k3,v23"));

        Map<String, String> map = table2.mapToPair((PairFunction<String, String, String>) s -> {
            int pos = s.indexOf(",");
            return new Tuple2<>(s.substring(0, pos), s.substring(pos + 1));
        }).collectAsMap();

        Broadcast<Map<String, String>> broadcastMap = sparkContext.broadcast(map);

        JavaRDD<Tuple2<String, Tuple2<String, String>>> join = table1.mapToPair((PairFunction<String, String, String>) s -> {
            int pos = s.indexOf(",");
            return new Tuple2<>(s.substring(0, pos), s.substring(pos + 1));
        }).mapPartitions((FlatMapFunction<Iterator<Tuple2<String, String>>, Tuple2<String, Tuple2<String, String>>>) tuple2Iterator -> {
            Map<String, String> bMap = broadcastMap.value();
            List<Tuple2<String, Tuple2<String, String>>> results = new ArrayList<>();
            while (tuple2Iterator.hasNext()) {
                Tuple2<String, String> tuple2 = tuple2Iterator.next();
                if (bMap.containsKey(tuple2._1)) {
                    results.add(new Tuple2<>(tuple2._1, new Tuple2<>(tuple2._2, bMap.get(tuple2._1))));
                }
            }

            return results.iterator();
        });

        join.saveAsTextFile("/tmp/spark/MapJoinApp-" + System.currentTimeMillis());

        sparkSession.stop();
    }

}
