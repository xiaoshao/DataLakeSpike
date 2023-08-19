package com.zw.sdk.utils;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

public class SDKConst {
    public static String catalog = "sdk_catalog";

    public static String cow_table_name = "cow_sdk_table";
    public static String mor_table_name = "mor_sdk_table";
    private static final String COW = HoodieTableType.COPY_ON_WRITE.name();
    private static final String MOR = HoodieTableType.MERGE_ON_READ.name();
    public static String hudi_data_location = "/Users/shaozengwei/projects/data/hudi";

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
        return Paths.get(hudi_data_location, cow_table_name).toString();
    }

    public static String getMorHudiTablePath() {
        return Paths.get(hudi_data_location, mor_table_name).toString();
    }


    private static FileSystem createFileSystem(Configuration configuration) {
        return FSUtils.getFs(hudi_data_location, configuration);
    }

    public static void initCopyOnWriteHudiTable(Configuration configuration) throws IOException {
        FileSystem fileSystem = createFileSystem(configuration);

        Path hudiDataPath = new Path(hudi_data_location);
        if (!fileSystem.exists(hudiDataPath)) {
            fileSystem.mkdirs(hudiDataPath);
        }

        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(COW)
                .setTableName(cow_table_name)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new Configuration(), getCowHudiTablePath());
    }


    public static void initMORHudiTable(Configuration configuration) throws IOException {
        FileSystem fileSystem = createFileSystem(configuration);

        Path hudiDataPath = new Path(hudi_data_location);
        if (!fileSystem.exists(hudiDataPath)) {
            fileSystem.mkdirs(hudiDataPath);
        }

        HoodieTableMetaClient.withPropertyBuilder()
                .setTableType(MOR)
                .setTableName(mor_table_name)
                .setPayloadClassName(HoodieAvroPayload.class.getName())
                .initTable(new Configuration(), getMorHudiTablePath());
    }


    public static HoodieWriteConfig createMorHoodieWriteConfig(String tableName) {
        return HoodieWriteConfig.newBuilder()
                .withPath(getMorHudiTablePath())
                .withSchema(hudi_schema.toString(true))
                .withParallelism(2, 2)
                .forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .build();
    }

    public static HoodieWriteConfig createCowHoodieWriteConfig(String tableName) {
        return HoodieWriteConfig.newBuilder()
                .withPath(getCowHudiTablePath())
                .withSchema(hudi_schema.toString(true))
                .withParallelism(2, 2)
                .forTable(tableName)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .build();
    }
}
