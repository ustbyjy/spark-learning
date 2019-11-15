package com.yjy.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class RDDTransformationsApp {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        sparkSession = SparkSession.builder().appName("RDDTransformationsApp")
                .master("local[2]").getOrCreate();
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        testDistinct();

        sparkSession.stop();
    }

    private static void testDistinct() {
        List<Integer> data1 = Arrays.asList(1, 2, 3);
        List<Integer> data2 = Arrays.asList(2, 3, 4);

        JavaRDD<Integer> rdd1 = sparkContext.parallelize(data1);
        JavaRDD<Integer> rdd2 = sparkContext.parallelize(data2);

        System.out.println(rdd1.union(rdd2).distinct().collect());
    }

}
