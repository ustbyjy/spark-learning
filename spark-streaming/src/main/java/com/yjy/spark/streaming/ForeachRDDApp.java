package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

public class ForeachRDDApp {

    public static void main(String[] args) throws InterruptedException {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("ForeachRDDApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("vhost1", 9999);

        JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<String, Integer>(word, 1));

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey(Integer::sum);

        // 在driver端创建Connection，在executor端使用，会报Task not serializable错误
//        wordCounts.foreachRDD(rdd -> {
//            Connection connection = createConnection();
//            rdd.foreach(record -> {
//                String sql = String.format("insert into word_count(word, count) values('%s', %d)", record._1, record._2);
//                connection.createStatement().execute(sql);
//                connection.close();
//            });
//        });

        // 为每条记录创建新的连接，会造成不必要的高开销，降低系统的吞吐量
//        wordCounts.foreachRDD(rdd -> {
//            rdd.foreach(record -> {
//                Connection connection = createConnection();
//                String sql = String.format("insert into word_count(word, count) values('%s', %d)", record._1, record._2);
//                connection.createStatement().execute(sql);
//                connection.close();
//            });
//        });

        // 每个rdd分区创建一个Connection
        wordCounts.foreachRDD(rdd -> {
            rdd.foreachPartition(records -> {
                Connection connection = createConnection();
                records.forEachRemaining(record -> {
                    try {
                        String sql = String.format("insert into word_count(word, count) values('%s', %d)", record._1, record._2);
                        connection.createStatement().execute(sql);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

                connection.close();
            });
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static Connection createConnection() throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://vhost1:3306/spark", "root", "root123..");

        return connection;
    }

}
