package com.yjy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSessionApp {

    public static void main(String[] args) {
        String path = SparkSessionApp.class.getClassLoader().getResource("") + "people.json";

        SparkSession sparkSession = SparkSession.builder().appName("SparkSessionApp").master("local[2]").getOrCreate();

        Dataset<Row> people = sparkSession.read().json(path);

        people.printSchema();
        people.show();

        // 注册临时表
        people.createOrReplaceTempView("people");
        sparkSession.sql("select * from people").show();

        sparkSession.stop();
    }

}
