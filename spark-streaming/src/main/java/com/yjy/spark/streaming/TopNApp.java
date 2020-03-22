package com.yjy.spark.streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * 每隔10秒统计最近60秒每个品类销量最高的top 3个商品
 */
public class TopNApp {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("TopNApp");
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
        JavaReceiverInputDStream<String> inputDStream = streamingContext.socketTextStream("vhost1", 29999);
        inputDStream.mapToPair(x -> {
            String[] split = x.split(",");
            return new Tuple2<>(split[1] + "_" + split[2], 1);
        })
                .reduceByKeyAndWindow(Integer::sum, Durations.seconds(60), Durations.seconds(10))
                .foreachRDD(rdd -> {
                    JavaRDD<Row> rowRDD = rdd.map(tuple -> {
                        String[] split = tuple._1.split("_");
                        String category = split[0];
                        String product = split[1];
                        Integer count = tuple._2;

                        return RowFactory.create(category, product, count);
                    });

                    List<StructField> structFields = Arrays.asList(
                            DataTypes.createStructField("category", DataTypes.StringType, true),
                            DataTypes.createStructField("product", DataTypes.StringType, true),
                            DataTypes.createStructField("count", DataTypes.IntegerType, true)
                    );
                    StructType schema = DataTypes.createStructType(structFields);

                    HiveContext hiveContext = new HiveContext(rdd.context());
                    Dataset<Row> dataFrame = hiveContext.createDataFrame(rowRDD, schema);
                    dataFrame.registerTempTable("product_count");

                    String sql = "select category,product,count from (select category,product,count,row_number() over(partition by category order by count desc) rank from product_count) tmp where tmp.rank <= 3";
                    Dataset<Row> result = hiveContext.sql(sql);
                    result.show();
                });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

}
