package com.yjy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;

public class SQLContextApp {

    public static void main(String[] args) {
        String path = args.length > 0 ? args[0] : SQLContextApp.class.getClassLoader().getResource("") + "people.json";

        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("SQLContextApp")
                .setMaster("local[2]");

        SparkContext sparkContext = new SparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sparkContext);

        Dataset people = sqlContext.read().format("json").load(path);
        people.printSchema();
        people.show();

        sparkContext.stop();
    }

}
