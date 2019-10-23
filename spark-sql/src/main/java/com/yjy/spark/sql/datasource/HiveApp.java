package com.yjy.spark.sql.datasource;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.List;

public class HiveApp {

    public static void main(String[] args) {
        // 需要指定访问HDFS的用户，否则抛出：Permission denied: user=Administrator, access=WRITE, inode="/user/hive/warehouse/student":root:supergroup:drwxrwxr-x
        System.setProperty("HADOOP_USER_NAME", "root");

        SparkSession sparkSession = SparkSession.builder()
                .appName("HiveApp")
                .master("local[2]")
                .enableHiveSupport()
                .getOrCreate();

        List<Student> studentList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            studentList.add(new Student("100" + i, RandomStringUtils.randomAlphabetic(5)));
        }

        Dataset<Row> studentDF = sparkSession.createDataFrame(studentList, Student.class);

        // Saving data in the Hive serde table `default`.`student` is not supported yet. Please use the insertInto() API as an alternative..;
//        studentDF.write().mode(SaveMode.Append).saveAsTable("student");
        studentDF.write().mode(SaveMode.Append).insertInto("student");

        sparkSession.stop();
    }

    @Data
    @AllArgsConstructor
    public static class Student {
        private String id;
        private String name;
    }

}
