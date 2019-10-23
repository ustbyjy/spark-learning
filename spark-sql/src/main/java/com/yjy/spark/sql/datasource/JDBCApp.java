package com.yjy.spark.sql.datasource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class JDBCApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveApp")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        Dataset<Row> jdbcDF = sparkSession.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://vhost1:3306/spark")
                .option("dbtable", "spark.person")
                .option("user", "root")
                .option("password", "root123..")
                .load();

        Properties connectionProperties = new Properties();
        connectionProperties.put("user", "root");
        connectionProperties.put("password", "root123..");
        Dataset<Row> jdbcDF2 = sparkSession.read()
                .jdbc("jdbc:mysql://vhost1:3306/spark", "spark.person", connectionProperties);

        jdbcDF.write().mode(SaveMode.Append)
                .format("jdbc")
                .option("url", "jdbc:mysql://vhost1:3306/spark")
                .option("dbtable", "spark.person")
                .option("user", "root")
                .option("password", "root123..")
                .save();

        jdbcDF2.write().mode(SaveMode.Append)
                .jdbc("jdbc:mysql://vhost1:3306/spark", "spark.person", connectionProperties);

        sparkSession.stop();
    }

}
