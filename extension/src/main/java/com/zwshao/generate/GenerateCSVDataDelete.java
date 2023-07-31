package com.zwshao.generate;

import com.zwshao.generate.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenerateCSVDataDelete {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.createSparkSession("origin");

        Dataset<Row> csv = session.read().format("csv")
                .schema(SparkUtils.createSchema()).option("delimiter", "|")
                .load("/Users/shaozengwei/projects/tpcds-kit/output");

        csv.limit(100000).
                write().
                format("csv").
                save("/Users/shaozengwei/projects/tpcds-kit/delete");
    }
}
