package com.zwshao.spark.write;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static com.zwshao.spark.utils.SparkUtils.createTablePath;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME;
import static org.apache.hudi.keygen.constant.KeyGeneratorOptions.RECORDKEY_FIELD_NAME;

public class HudiWriteApplication {
    public static void main(String[] args) {
        String originDataPath = args[0];

        SparkSession session = SparkUtils.createSparkSession("hudi_write_origin");

        Dataset<Row> hudi = session.read().format("csv").option("delimiter", "|").schema(SparkUtils.createSchema()).load(originDataPath);

        hudi.filter("ss_sold_date_sk is not null and ts is not null").write().format("hudi")
                .option("hoodie.table.name", SparkUtils.CATALOG_TABLE)
                .option(RECORDKEY_FIELD_NAME.key(), "ss_sold_date_sk")
                .option(PARTITIONPATH_FIELD_NAME.key(), "ss_sold_date_sk")
                .mode(SaveMode.Overwrite)
                .save("/Users/shaozengwei/projects/data/hudi/hudi_table");
    }
}
