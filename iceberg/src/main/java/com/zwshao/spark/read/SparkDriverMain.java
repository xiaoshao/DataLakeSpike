package com.zwshao.spark.read;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriverMain {
    public static void main(String[] args) {

        SparkConf conf = SparkUtils.createIcebergConf();

        SparkSession session = SparkUtils.createLocalSession(conf, "local_app");

        Dataset<Row> iceberg = session.sql("SELECT * FROM iceberg.zw.first_iceberg");

        iceberg.write().format("console").save();
    }
}
