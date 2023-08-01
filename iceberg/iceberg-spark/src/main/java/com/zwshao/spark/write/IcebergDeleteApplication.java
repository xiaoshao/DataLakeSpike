package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergDeleteApplication {
    public static void main(String[] args) {
        String deleteDataPath = args[0];
        String icebergLocation = args[1];

        SparkSession sparkSession = SparkUtils.createLocalSession(new SparkConf(), "iceberg_delete_application");

        Dataset<Row> deleteData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(deleteDataPath);

        deleteData.write().format("iceberg").save(icebergLocation);
    }
}
