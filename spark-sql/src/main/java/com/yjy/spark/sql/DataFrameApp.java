package com.yjy.spark.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * DataFrame API基本操作
 */
public class DataFrameApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("DataFrameApp")
                .master("local[2]").getOrCreate();

        String path = SparkSessionApp.class.getClassLoader().getResource("") + "people.json";

        Dataset<Row> peopleDataFrame = sparkSession.read().format("json").load(path);

        // desc people;
        peopleDataFrame.printSchema();

        // select * from people;
        peopleDataFrame.show();

        // select name from people;
        peopleDataFrame.select("name").show();

        // select name, (age + 10) as age2 from people;
        peopleDataFrame.select(peopleDataFrame.col("name"), peopleDataFrame.col("age").$plus(10).as("age2")).show();

        // select * from people where age > 19;
        peopleDataFrame.filter(peopleDataFrame.col("age").gt(19)).show();

        // select age,count(age) from people group by age;
        peopleDataFrame.groupBy("age").count().show();

        sparkSession.stop();
    }

}
