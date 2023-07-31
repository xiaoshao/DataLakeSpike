package com.zwshao.generate;

import com.zwshao.generate.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenerateCSVDataUpdate {
    public static void main(String[] args) {
        SparkSession session = SparkUtils.createSparkSession("origin");

        Dataset<Row> csv = session.read().format("csv")
                .schema(SparkUtils.createSchema()).option("delimiter", "|")
                .load("/Users/shaozengwei/projects/tpcds-kit/output");

        Dataset<Row> limitedData = csv.limit(100000);
        limitedData.registerTempTable("limit_table");
        Dataset<Row> result4Update = session.sql(
                "    select ss_sold_date_sk          " +
                " ss_sold_time_sk           ," +
                " ss_item_sk                ," +
                " ss_customer_sk            ," +
                " ss_cdemo_sk               ," +
                " ss_hdemo_sk               ," +
                " ss_addr_sk                ," +
                " ss_store_sk               ," +
                " ss_promo_sk               ," +
                " ss_ticket_number + 10     ," +
                " ss_quantity      + 10     ," +
                " ss_wholesale_cost+ 10     ," +
                " ss_list_price    + 10     ," +
                " ss_sales_price   + 10     ," +
                " ss_ext_discount_amt + 10  ," +
                " ss_ext_sales_price  + 10  ," +
                " ss_ext_wholesale_cost + 10," +
                " ss_ext_list_price  + 10   ," +
                " ss_ext_tax + 10           ," +
                " ss_coupon_amt  + 10       ," +
                " ss_net_paid   + 10        ," +
                " ss_net_paid_inc_tax + 10  ," +
                " ss_net_profit   + 10          from limit_table");

        result4Update.write().format("csv").save("/Users/shaozengwei/projects/tpcds-kit/update");
    }
}
