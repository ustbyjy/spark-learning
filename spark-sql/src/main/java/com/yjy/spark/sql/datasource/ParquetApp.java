package com.yjy.spark.sql.datasource;


import com.yjy.spark.sql.SparkSessionApp;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ParquetApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("ParquetApp")
                .master("local[2]").getOrCreate();

        String path = SparkSessionApp.class.getClassLoader().getResource("") + "users.parquet";

        Dataset<Row> userDF = sparkSession.read().parquet(path);
        userDF.printSchema();
        userDF.show();
        userDF.select("name", "favorite_color").show();
        userDF.select("name", "favorite_color").write().format("json").save("/tmp/spark/users.json");

        sparkSession.stop();
    }

}
