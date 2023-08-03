package com.zwshao.spark.read.spark.utils;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkUtils {

    public static final String CATALOG_NAME = "zwshao";
    public static final String MOR_ICEBERG_TABLE_NAME = "iceberg_mor";

    public static final String CATALOG_TABLE = CATALOG_NAME + "." + MOR_ICEBERG_TABLE_NAME;

    public static SparkSession createLocalSession(SparkConf conf, String app) {
        return SparkSession.builder()
                .config(conf)
                .appName(app)
                .master("local[*]")
                .getOrCreate();
    }

    public static SparkConf createIcebergConf() {
        SparkConf conf = new SparkConf();
        conf.set("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog");
        conf.set("spark.sql.catalog." + CATALOG_NAME + ".type", "hadoop");
        conf.set("spark.sql.catalog." + CATALOG_NAME + ".warehouse", "hdfs://localhost:9000/srv/iceberg");
        return conf;
    }

    public static StructType createSchema() {

//        create table tpcds.store_sales
//                (
//                        ss_sold_date_sk           integer                       ,
//                        ss_sold_time_sk           integer                       ,
//                        ss_item_sk                integer               not null,
//                ss_customer_sk            integer                       ,
//                ss_cdemo_sk               integer                       ,
//                ss_hdemo_sk               integer                       ,
//                ss_addr_sk                integer                       ,
//                ss_store_sk               integer                       ,
//                ss_promo_sk               integer                       ,
//                ss_ticket_number          integer               not null,
//                ss_quantity               integer                       ,
//                ss_wholesale_cost         decimal(7,2)                  ,
//                ss_list_price             decimal(7,2)                  ,
//                ss_sales_price            decimal(7,2)                  ,
//                ss_ext_discount_amt       decimal(7,2)                  ,
//                ss_ext_sales_price        decimal(7,2)                  ,
//                ss_ext_wholesale_cost     decimal(7,2)                  ,
//                ss_ext_list_price         decimal(7,2)                  ,
//                ss_ext_tax                decimal(7,2)                  ,
//                ss_coupon_amt             decimal(7,2)                  ,
//                ss_net_paid               decimal(7,2)                  ,
//                ss_net_paid_inc_tax       decimal(7,2)                  ,
//                ss_net_profit             decimal(7,2)                  ,
//                primary key (ss_item_sk, ss_ticket_number)
//);
        StructType schema = new StructType(new StructField[]{
                new StructField("ss_sold_date_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_sold_time_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_item_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_customer_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_cdemo_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_hdemo_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_addr_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_store_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_promo_sk", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_ticket_number", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_quantity", DataTypes.IntegerType, true, Metadata.empty()),
                new StructField("ss_wholesale_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_list_price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_sales_price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_ext_discount_amt", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_ext_sales_price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_ext_wholesale_cost", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_ext_list_price", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_ext_tax", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_coupon_amt", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_net_paid", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_net_paid_inc_tax", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("ss_net_profit", DataTypes.DoubleType, true, Metadata.empty())
        });

        return schema;
    }

}
