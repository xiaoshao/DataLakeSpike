package com.zwshao.spark.write;

import com.zwshao.spark.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static com.zwshao.spark.utils.SparkUtils.createTablePath;

public class HudiUpdateApplication {
    public static void main(String[] args) {
        String originDataPath = args[0];

        SparkSession session = SparkUtils.createSparkSession("hudi_update");

        Dataset<Row> hudi = session.read().format("csv").schema(SparkUtils.createSchema()).load(originDataPath);

        session.sql(createTablePath);

        hudi.write().format("hudi").mode(SaveMode.Append).save(SparkUtils.CATALOG_TABLE);
    }
}
