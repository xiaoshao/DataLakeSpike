package com.zwshao.sdk.utils;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

public class IcebergConst {

    public static String iceberg_data_location = "";

    public static String icebergNamespace = "iceberg_namespace";

    public static String morTableName = "iceberg_mor_table_name";
    public static String cowTableName = "iceberg_cow_table_name";

    public static Schema schema =  new Schema(
                Types.NestedField.required(1,"ss_sold_date_sk", Types.IntegerType.get().get()),
                Types.NestedField.required(2,"ss_sold_time_sk", Types.IntegerType.get() ),
                Types.NestedField.required(3,"ss_item_sk", Types.IntegerType.get() ),
                Types.NestedField.required(4,"ss_customer_sk", Types.IntegerType.get()),
                Types.NestedField.required(5,"ss_cdemo_sk", Types.IntegerType.get()),
                Types.NestedField.required(6,"ss_hdemo_sk", Types.IntegerType.get()),
                Types.NestedField.required(7,"ss_addr_sk", Types.IntegerType.get()),
                Types.NestedField.required(8,"ss_store_sk", Types.IntegerType.get()),
                Types.NestedField.required(9,"ss_promo_sk", Types.IntegerType.get()),
                Types.NestedField.required(10,"ss_ticket_number", Types.IntegerType.get()),
                Types.NestedField.required(11,"ss_quantity", Types.IntegerType.get()),
                Types.NestedField.required(12,"ss_wholesale_cost", Types.DoubleType.get()),
                Types.NestedField.required(13,"ss_list_price", Types.DoubleType.get()),
                Types.NestedField.required(14,"ss_sales_price", Types.DoubleType.get()),
                Types.NestedField.required(15,"ss_ext_discount_amt", Types.DoubleType.get()),
                Types.NestedField.required(16,"ss_ext_sales_price", Types.DoubleType.get()),
                Types.NestedField.required(17,"ss_ext_wholesale_cost", Types.DoubleType.get()),
                Types.NestedField.required(18,"ss_ext_list_price", Types.DoubleType.get()),
                Types.NestedField.required(19,"ss_ext_tax", Types.DoubleType.get()),
                Types.NestedField.required(20,"ss_coupon_amt", Types.DoubleType.get()),
                Types.NestedField.required(21,"ss_net_paid", Types.DoubleType.get()),
                Types.NestedField.required(22,"ss_net_paid_inc_tax", Types.DoubleType.get()),
                Types.NestedField.required(23,"ss_net_profit", Types.DoubleType.get())
        );

}
