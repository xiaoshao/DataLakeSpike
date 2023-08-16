package com.zwshao.sdk.utils;

import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Predicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;

import java.util.List;

public class IcebergTableOperation {

    private Catalog catalog;
    private String warehousePath = "hdfs://localhost:9000/srv/iceberg";
    public IcebergTableOperation() {
        Configuration configuration = new Configuration();

        this.catalog = new HadoopCatalog(configuration, warehousePath);
    }

    public Table createTable(String namespace, String tableName, Schema schema, PartitionSpec partitionSpec) {

        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        if (isTableExists(namespace, tableName)) {
            return this.loadTable(namespace, tableName);
        } else {
            return catalog.createTable(tableIdentifier, schema, partitionSpec);
        }
    }

    public Table loadTable(String namespace, String tableName) {
        return catalog.loadTable(TableIdentifier.of(namespace, tableName));
    }

    public CloseableIterable<CombinedScanTask> loadData(Table table, Predicate expression, String column1, String column2) {
        TableScan tableScan = table.newScan();
        TableScan scan = tableScan.filter(expression).select(column1, column2);

        Schema schema = scan.schema();
        System.out.println(schema);

        return scan.planTasks();
    }

    public List<Record> loadDataRecord(Table table, Predicate filterExpression, String column1, String column2, String column3) {
        IcebergGenerics.ScanBuilder read = IcebergGenerics.read(table);
        CloseableIterable<Record> build = read.where(filterExpression).select(column1, column2, column3).build();

        return Lists.newArrayList(build.iterator());
    }


    public void addData(Table table){

    }
    private boolean isTableExists(String namespace, String tableName) {
        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);
        Configuration configuration = new Configuration();


        HadoopCatalog catalog1 = new HadoopCatalog(configuration, warehousePath);
        Catalog catalog = catalog1;

        try {
            catalog.loadTable(tableIdentifier);
            return true;
        } catch (NoSuchTableException exception) {
            return false;
        }
    }
}
