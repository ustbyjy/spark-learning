package com.yjy.spark.core;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class RDDActionsApp {

    private static SparkSession sparkSession = null;
    private static JavaSparkContext sparkContext = null;

    public static void main(String[] args) {
        sparkSession = SparkSession.builder().appName("RDDActionsApp")
                .master("local[2]").getOrCreate();
        sparkContext = new JavaSparkContext(sparkSession.sparkContext());

        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        System.out.println("data: " + data);

        JavaRDD<Integer> rdd = sparkContext.parallelize(data);

        System.out.println("collect: " + rdd.collect());
        System.out.println("count: " + rdd.count());
        System.out.println("take: " + rdd.take(3));
        System.out.println("min: " + rdd.min(new IntegerComparator()));
        System.out.println("max: " + rdd.max(new IntegerComparator()));
        System.out.println("sum: " + rdd.reduce(Integer::sum));

        rdd.foreach(new PrintFunction());

        sparkSession.stop();
    }

    private static class IntegerComparator implements Comparator<Integer>, Serializable {
        private static final long serialVersionUID = 8907898824948049818L;

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 - o2;
        }
    }

    private static class PrintFunction implements VoidFunction<Integer>, Serializable {
        private static final long serialVersionUID = 1520140574583534948L;

        @Override
        public void call(Integer i) {
            System.out.println(i);
        }
    }

}
