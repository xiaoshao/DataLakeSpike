package com.zw.sdk.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class SDKConst {
    public static String catalog = "sdk_catalog";

    public static String cow_table_name = "cow_sdk_table";
    public static String mor_table_name = "mor_sdk_table";
    private static final String COW = HoodieTableType.COPY_ON_WRITE.name();
    private static final String MOR = HoodieTableType.MERGE_ON_READ.name();
    public static String location = "/Users/shaozengwei/projects/data/hudi";

    public static String recordKeyFields = "ids";

    public static Schema hudi_schema;

    static {
        ArrayList<Schema.Field> fields = new ArrayList<>();
        fields.add(new Schema.Field("ss_sold_date_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_sold_time_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_item_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_customer_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_cdemo_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_hdemo_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_addr_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_store_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_promo_sk", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_ticket_number", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_quantity", Schema.create(Schema.Type.INT), "desc", null));
        fields.add(new Schema.Field("ss_wholesale_cost", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_list_price", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_sales_price", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_ext_discount_amt", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_ext_sales_price", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_ext_wholesale_cost", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_ext_list_price", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_ext_tax", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_coupon_amt", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_net_paid", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_net_paid_inc_tax", Schema.create(Schema.Type.DOUBLE), "desc", null));
        fields.add(new Schema.Field("ss_net_profit", Schema.create(Schema.Type.DOUBLE), "desc", null));
        hudi_schema = Schema.createRecord("hudi_name", "hudi_doc", "hudi_namespace", false,fields);
    }

    public static String getCowHudiTablePath() {
        return Paths.get(location, cow_table_name).toString();
    }

    public static String getMorHudiTablePath() {
        return Paths.get(location, mor_table_name).toString();
    }


    public static FileSystem createFileSystem(Configuration configuration) {
        return FSUtils.getFs(location, configuration);
    }

    public static void initCopyOnWriteHudiTable(Configuration configuration) throws IOException {
        FileSystem fileSystem = createFileSystem(configuration);

        Path cowHudiTablePath = new Path(location);
        if (!fileSystem.exists(cowHudiTablePath)) {
            fileSystem.mkdirs(cowHudiTablePath);
        }

        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(COW)
                .setTableName(cow_table_name)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new Configuration(), getCowHudiTablePath());
    }



}
