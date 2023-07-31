package com.zwshao.generate;

import com.zwshao.generate.utils.SparkUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class GenerateData {

    public static void main(String[] args) {
        String inputDir = args[0];
        String outputDir = args[1];

        SparkSession session = SparkUtils.createSparkSession("origin");

        Dataset<Row> originData = session.read().format("csv")
                .schema(SparkUtils.createSchema()).option("delimiter", "|")
                .load(inputDir);

        originData.write().format("csv")
                .option("mode", "overwrite")
                .save(outputDir + "/output1");

        originData.registerTempTable("limit_table");

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
                        " ss_net_profit   + 10          " +
                        " from limit_table " +
                        " order by ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk" +
                        " limit 100000, 100000");

        result4Update.write().format("csv").save("outputDir/update");

        Dataset<Row> result4Delete = session.sql(
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
                        " ss_net_profit   + 10          " +
                        " from limit_table " +
                        " order by ss_sold_date_sk, ss_sold_time_sk, ss_item_sk, ss_customer_sk" +
                        " limit 100000");

        result4Delete.limit(100000).
                write().
                format("csv").
                save(outputDir + "/delete");
    }
}
