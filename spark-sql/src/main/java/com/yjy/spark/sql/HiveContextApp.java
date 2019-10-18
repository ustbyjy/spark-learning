package com.yjy.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.hive.HiveContext;

public class HiveContextApp {

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf();
//        sparkConf.setAppName("HiveContextApp").setMaster("local[2]");

        SparkContext sparkContext = new SparkContext(sparkConf);

        HiveContext hiveContext = new HiveContext(sparkContext);

        hiveContext.table("student").show();

        sparkContext.stop();
    }

}
