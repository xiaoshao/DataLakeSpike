package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class IcebergDeleteApplication {
    public static void main(String[] args) {
        String deleteDataPath = args[0];

        SparkConf conf = SparkUtils.createIcebergConf();
        SparkSession sparkSession = SparkUtils.createLocalSession(conf, "iceberg_origin_application");

        Dataset<Row> deleteData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(deleteDataPath);

        deleteData.registerTempTable("delete_data");
        sparkSession.sql(SparkUtils.ICEBERG_CREATE_TABLE_SQL);

        Dataset<Row> sql = sparkSession.sql("delete from " + SparkUtils.CATALOG_TABLE + " where " +
                "(ss_sold_date_sk, ss_sold_time_sk, ss_item_sk) in ( select ss_sold_date_sk, ss_sold_time_sk, ss_item_sk from delete_data)");
        sql.write().format("console").save();
    }
}
