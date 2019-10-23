package com.yjy.spark.sql.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HiveMySQLApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveApp")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> hiveDF = sparkSession.table("student");

        Dataset<Row> mysqlDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://vhost1:3306/spark")
                .option("dbtable", "spark.person")
                .option("user", "root")
                .option("password", "root123..")
                .load();

        hiveDF.join(mysqlDF, "id").show();

        sparkSession.stop();
    }

}
