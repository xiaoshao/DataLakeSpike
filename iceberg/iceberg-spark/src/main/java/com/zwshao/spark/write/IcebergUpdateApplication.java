package com.zwshao.spark.write;

import com.zwshao.spark.read.spark.utils.SparkUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class IcebergUpdateApplication {
    public static void main(String[] args) {
        String updateDataPath = args[0];
        String icebergLocation = args[1];

        SparkConf conf = SparkUtils.createIcebergConf();
        SparkSession sparkSession = SparkUtils.createLocalSession(conf, "iceberg_origin_application");

        Dataset<Row> originData = sparkSession.read().format("csv").schema(SparkUtils.createSchema()).load(updateDataPath);

        sparkSession.sql("create table if not exists " + SparkUtils.CATALOG_TABLE + "(" +
                "                ss_sold_date_sk           integer                       ," +
                "                ss_sold_time_sk           integer                       ," +
                "                ss_item_sk                integer               not null," +
                "                ss_customer_sk            integer                       ," +
                "                ss_cdemo_sk               integer                       ," +
                "                ss_hdemo_sk               integer                       ," +
                "                ss_addr_sk                integer                       ," +
                "                ss_store_sk               integer                       ," +
                "                ss_promo_sk               integer                       ," +
                "                ss_ticket_number          integer               not null," +
                "                ss_quantity               integer                       ," +
                "                ss_wholesale_cost         decimal(7,2)                  ," +
                "                ss_list_price             decimal(7,2)                  ," +
                "                ss_sales_price            decimal(7,2)                  ," +
                "                ss_ext_discount_amt       decimal(7,2)                  ," +
                "                ss_ext_sales_price        decimal(7,2)                  ," +
                "                ss_ext_wholesale_cost     decimal(7,2)                  ," +
                "                ss_ext_list_price         decimal(7,2)                  ," +
                "                ss_ext_tax                decimal(7,2)                  ," +
                "                ss_coupon_amt             decimal(7,2)                  ," +
                "                ss_net_paid               decimal(7,2)                  ," +
                "                ss_net_paid_inc_tax       decimal(7,2)                  ," +
                "                ss_net_profit             decimal(7,2)                  ," +
                "                primary key (ss_item_sk, ss_ticket_number)" +
                ") using iceberg");

        originData
                .write()
                .format("iceberg")
                .mode(SaveMode.Overwrite)
                .save(SparkUtils.MOR_ICEBERG_TABLE_NAME);
    }
}
