package com.zw.sdk.utils;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;

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
        fields.add(new Schema.Field("id", Schema.create(Schema.Type.STRING), "desc", null));
        fields.add(new Schema.Field("name", Schema.create(Schema.Type.STRING), "desc", null));
        hudi_schema = Schema.createRecord(fields);
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
