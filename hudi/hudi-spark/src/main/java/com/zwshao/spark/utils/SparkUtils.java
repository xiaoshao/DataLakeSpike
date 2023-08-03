package com.zwshao.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkUtils {

    public static final String CATALOG_NAME = "zwshao";
    public static final String MOR_HUDI_TABLE_NAME = "hudi_mor";

    public static final String CATALOG_TABLE = CATALOG_NAME + "." + MOR_HUDI_TABLE_NAME;

    public static SparkSession createSparkSession(String applicationName) {
        SparkConf conf = new SparkConf();
        conf.setAppName(applicationName);
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        conf.set("spark.sql.catalog." + CATALOG_NAME, "org.apache.spark.sql.hudi.catalog.HoodieCatalog");
        return SparkSession.builder().config(conf).master("local[*]").getOrCreate();
    }
}
