package com.yjy.spark.sql;

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
        System.setProperty("user.name", "root");

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
        studentDF.write().format("parquet").mode(SaveMode.Append).saveAsTable("student_1");

        sparkSession.sql("show tables").show();
        sparkSession.sql("select * from student_1").show(200);

        sparkSession.stop();
    }

    @Data
    @AllArgsConstructor
    public static class Student {
        private String id;
        private String name;
    }

}
