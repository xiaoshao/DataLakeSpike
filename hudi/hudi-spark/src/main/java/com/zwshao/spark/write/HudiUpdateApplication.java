package com.zwshao.spark.write;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class HudiUpdateApplication {
    public static void main(String[] args) {

        SparkSession session = SparkUtils.createSparkSession("HudiWriteApplication");

        Dataset<Row> inputData = session.read().format("csv").schema(new StructType()).load("");

        inputData.write().format("hudi").option("", "").save("");
    }
}
