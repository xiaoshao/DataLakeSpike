package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static com.zwshao.spark.read.spark.utils.SparkUtils.ICEBERG_CREATE_TABLE_SQL;

public class IcebergWriteApplication {

    public static void main(String[] args) {
        String originDataPath = args[0];

        SparkConf conf = SparkUtils.createIcebergConf();
        SparkSession sparkSession = SparkUtils.createLocalSession(conf, "iceberg_origin_application");

        Dataset<Row> originData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(originDataPath);


        sparkSession.sql(ICEBERG_CREATE_TABLE_SQL);

        originData.writeTo(SparkUtils.CATALOG_TABLE).createOrReplace();
    }
}
