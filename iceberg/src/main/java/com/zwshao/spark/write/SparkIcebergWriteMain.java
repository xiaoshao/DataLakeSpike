package com.zwshao.spark.write;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.iceberg.spark.SparkUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkIcebergWriteMain {

    public static void main(String[] args) {
        SparkConf conf = SparkUtils.createIcebergConf();

        SparkSession session = SparkUtils.createLocalSession(conf, "local-app");

        Dataset<Row> sql = session.sql("insert into iceberg.zw.first_iceberg(level, event_time, message_type) values('level_2', (timestamp '2023-12-12 10:10:10'), 'mes_type1')");

        sql.write().format("console").save();
    }
}
