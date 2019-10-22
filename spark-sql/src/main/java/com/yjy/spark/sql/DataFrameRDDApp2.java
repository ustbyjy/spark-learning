package com.yjy.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class DataFrameRDDApp2 {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("DataFrameRDDApp2")
                .master("local[2]").getOrCreate();

        String path = SparkSessionApp.class.getClassLoader().getResource("") + "info.txt";
        JavaRDD<String> peopleRDD = sparkSession.sparkContext().textFile(path, 1).toJavaRDD();

        String schemaString = "id name age";

        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD = peopleRDD.map(line -> {
            String[] attributes = line.split(",");
            return RowFactory.create(attributes[0], attributes[1].trim(), attributes[2]);
        });

        Dataset<Row> peopleDataFrame = sparkSession.createDataFrame(rowRDD, schema);

        peopleDataFrame.show();

        sparkSession.stop();
    }

}
