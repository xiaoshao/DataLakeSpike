package com.zwshao.spark.read;

import com.zwshao.spark.read.iceberg.IcebergTableOperation;
import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

import static com.zwshao.spark.read.spark.utils.SparkUtils.MOR_ICEBERG_TABLE_NAME;

public class IcebergReadSnapshotApplication {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.createLocalSession(SparkUtils.createIcebergConf(), "read_snapshot");
        Dataset<Row> snapshot_data = session.read().format("iceberg").load(MOR_ICEBERG_TABLE_NAME);
        snapshot_data.write().format("console").save();
    }
}
