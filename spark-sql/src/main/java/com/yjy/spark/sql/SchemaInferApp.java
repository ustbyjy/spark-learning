package com.yjy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SchemaInferApp {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("SchemaInferApp")
                .master("local[2]")
                .getOrCreate();

        String path = SparkSessionApp.class.getClassLoader().getResource("") + "json_schema_infer.json";

        Dataset<Row> df = sparkSession.read().format("json").load(path);

        df.printSchema();

        df.show();

        sparkSession.stop();
    }

}
