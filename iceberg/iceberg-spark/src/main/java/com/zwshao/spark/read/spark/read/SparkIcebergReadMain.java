package com.zwshao.spark.read.spark.read;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkIcebergReadMain {
    public static void main(String[] args) {

        SparkConf conf = SparkUtils.createIcebergConf();

        SparkSession session = SparkUtils.createLocalSession(conf, "read_iceberg");

        Dataset<Row> iceberg = session.read().format("iceberg").load("zwshao.iceberg");

        iceberg.show();
    }
}
