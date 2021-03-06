package com.yjy.spark.sql;

import lombok.Data;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

public class DataFrameRDDApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("DataFrameRDDApp")
                .master("local[2]").getOrCreate();

        String path = SparkSessionApp.class.getClassLoader().getResource("") + "info.txt";

        JavaRDD<Info> infoRDD = sparkSession.read().textFile(path).javaRDD()
                .map(line -> {
                    String[] parts = line.split(",");
                    Info info = new Info();
                    info.setId(Integer.parseInt(parts[0].trim()));
                    info.setName(parts[1]);
                    info.setAge(Integer.parseInt(parts[2].trim()));

                    return info;
                });

        Dataset<Row> infoDataFrame = sparkSession.createDataFrame(infoRDD, Info.class);

        infoDataFrame.createOrReplaceTempView("info");

        sparkSession.sql("SELECT name FROM info WHERE age BETWEEN 13 AND 30").show();

        Encoder<String> stringEncoder = Encoders.STRING();
        Dataset<String> namesByIndexDF = infoDataFrame.map(
                (MapFunction<Row, String>) row -> "Name: " + row.getString(2),
                stringEncoder);
        namesByIndexDF.show();

        Dataset<String> namesByFieldDF = infoDataFrame.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        namesByFieldDF.show();

        sparkSession.stop();
    }

    @Data
    public static class Info {
        private int id;
        private String name;
        private int age;
    }

}
