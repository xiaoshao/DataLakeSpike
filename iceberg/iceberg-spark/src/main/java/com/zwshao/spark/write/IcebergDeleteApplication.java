package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergDeleteApplication {
    public static void main(String[] args) {
        SparkConf conf = SparkUtils.createIcebergConf();
        SparkSession sparkSession = SparkUtils.createLocalSession(conf, "iceberg_delete_application");

        sparkSession.sql(SparkUtils.ICEBERG_CREATE_TABLE_SQL);

        Dataset<Row> sql = sparkSession.sql("delete from " + SparkUtils.CATALOG_TABLE);

        sql.write().format("console").save();
    }
}
