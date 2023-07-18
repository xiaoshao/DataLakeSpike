package com.zwshao.spark.read.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static SparkSession createLocalSession(SparkConf conf, String app) {
        return SparkSession.builder()
                .config(conf)
                .appName(app)
                .master("local[*]")
                .getOrCreate();
    }

    public static SparkConf createIcebergConf(){
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog.iceberg","org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog.iceberg.type", "hadoop");
        conf.set("spark.sql.catalog.iceberg.warehouse", "hdfs://localhost:9000/srv/iceberg");
        return conf;
    }
}
