package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergWriteApplication {

    public static void main(String[] args) {
        String originDataPath = args[0];
        String icebergLocation = args[1];

        SparkSession sparkSession = SparkUtils.createLocalSession(new SparkConf(), "iceberg_origin_application");

        Dataset<Row> originData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(originDataPath);

       originData.writeTo("")
    }
}
