package com.yjy.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class StateAirApp {

    public static void main(String[] args) {
        String file2015 = "Beijing_2015_HourlyPM25_created20160201.csv";
        String file2016 = "Beijing_2016_HourlyPM25_created20170201.csv";
        String file2017 = "Beijing_2017_HourlyPM25_created20170803.csv";
        String path = SparkSessionApp.class.getClassLoader().getResource("").toString();

        SparkSession sparkSession = SparkSession.builder().appName("StateAirApp").master("local[2]").getOrCreate();

        Dataset<Row> data2015 = sparkSession.read().option("header", "true").csv(path + file2015);
        Dataset<Row> data2016 = sparkSession.read().option("header", "true").csv(path + file2016);
        Dataset<Row> data2017 = sparkSession.read().option("header", "true").csv(path + file2017);

        sparkSession.sqlContext().udf().register("getGrade", (UDF1<String, String>) s -> {
            int value = Integer.parseInt(s.trim());
            if (value >= 0 && value <= 50) {
                return "健康";
            } else if (value <= 100) {
                return "中等";
            } else if (value <= 150) {
                return "对敏感人群不健康";
            } else if (value <= 200) {
                return "不健康";
            } else if (value <= 300) {
                return "非常不健康";
            } else if (value <= 500) {
                return "危险";
            } else {
                return "报表";
            }
        }, DataTypes.StringType);

        Dataset<Row> df1 = data2015.union(data2016).union(data2017);
        Dataset<Row> df2 = df1.withColumn("Grade", functions.callUDF("getGrade", df1.col("Value")));
        df2.printSchema();

        int count = (int) df2.count();
        df2.show(count);

        sparkSession.stop();
    }

}
