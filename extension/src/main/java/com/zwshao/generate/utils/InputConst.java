package com.zwshao.generate.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class InputConst {

    public static String ORIGIN_DATA_DIR = "";

    public static String DELETE_DATA_DIR = "";

    public static String UPDATE_DATA_DIR = "";

    public static SparkSession createGenerateSparkSession(String applicationName) {
        SparkConf conf = new SparkConf();
        return SparkSession.builder().config(conf).getOrCreate();
    }
}
