package com.zwshao.generate;

import com.zwshao.generate.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenerateCSVDataOrigin {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.createSparkSession("origin");

        Dataset<Row> csv = session.read().format("csv")
                .schema(SparkUtils.createSchema()).option("delimiter", "|")
                .load("/Users/shaozengwei/projects/tpcds-kit/output");

        csv.write().format("csv").option("mode", "overwrite").save("/Users/shaozengwei/projects/tpcds-kit/output1");
    }
}
