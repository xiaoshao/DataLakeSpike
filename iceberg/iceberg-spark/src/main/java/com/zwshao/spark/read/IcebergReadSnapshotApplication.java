package com.zwshao.spark.read;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.zwshao.spark.read.spark.utils.SparkUtils.MOR_ICEBERG_TABLE_NAME;

public class IcebergReadSnapshotApplication {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.createLocalSession(SparkUtils.createIcebergConf(), "read_snapshot");
        Dataset<Row> snapshot_data = session.read().format("iceberg").load(MOR_ICEBERG_TABLE_NAME);
        snapshot_data.write().format("console").save();
    }
}
