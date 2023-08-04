package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class IcebergUpdateApplication {
    public static void main(String[] args) {
        String updateDataPath = args[0];

        SparkConf conf = SparkUtils.createIcebergConf();
        SparkSession sparkSession = SparkUtils.createLocalSession(conf, "iceberg_origin_application");

        Dataset<Row> originData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(updateDataPath);

        sparkSession.sql(SparkUtils.ICEBERG_CREATE_TABLE_SQL);

        originData
                .write()
                .format("iceberg")
                .mode(SaveMode.Overwrite)
                .save(SparkUtils.CATALOG_TABLE);
    }
}
