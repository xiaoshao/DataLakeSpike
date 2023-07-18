package com.zwshao.spark.read;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class HudiReadIncrementApplication {

    public static void main(String[] args) {
        SparkSession session = SparkUtils.createSparkSession("HudiReadIncrementApplication");

        Dataset<Row> hudi = session.read().format("hudi").option("", "").load("");

        hudi.write().format("console").save();
    }
}
