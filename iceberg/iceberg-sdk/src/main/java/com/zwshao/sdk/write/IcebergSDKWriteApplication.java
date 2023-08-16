package com.zwshao.sdk.write;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

public class IcebergSDKWriteApplication {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        String warehousePath = "hdfs://host:8020/warehouse_path";
        HadoopCatalog catalog = new HadoopCatalog(conf, warehousePath);

        TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(""), "");

//        catalog.createTable(tableIdentifier);
    }
}
