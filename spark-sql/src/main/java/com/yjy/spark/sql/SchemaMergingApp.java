package com.yjy.spark.sql;

import lombok.Data;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class SchemaMergingApp {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SchemaMergingApp")
                .master("local[2]")
                .getOrCreate();

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("tmp/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("tmp/test_table/key=2");

        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("tmp/test_table");
        mergedDF.printSchema();

        spark.stop();
    }

    @Data
    public static class Square implements Serializable {
        private int value;
        private int square;
    }

    @Data
    public static class Cube implements Serializable {
        private int value;
        private int cube;
    }

}
