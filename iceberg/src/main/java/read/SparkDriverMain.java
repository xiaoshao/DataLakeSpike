package read;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkDriverMain {
    public static void main(String[] args) {

        SparkSession session = SparkSession.builder().appName("local-app").master("local[*]").getOrCreate().newSession();
        StructType schema = new StructType(new StructField[]{
                new StructField("id", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("name", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset<Row> csv = session.read().format("csv").schema(schema).load("./iceberg/input/input.csv");
        csv.write().format("console").save();
    }
}
