package com.zwshao.sdk.utils;

import com.google.common.collect.Iterables;
import com.zwshao.sdk.write.bean.IcebergDeleteRecord;
import org.apache.commons.compress.utils.Iterators;
import org.apache.commons.compress.utils.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.deletes.PositionDeleteWriter;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.Predicate;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.*;
import java.util.stream.LongStream;

import static com.zwshao.sdk.utils.IcebergConst.*;

public class IcebergTableOperation {

    private Catalog catalog;
    private String warehousePath = "/Users/shaozengwei/projects/data/iceberg";

    public IcebergTableOperation() {
        Configuration configuration = new Configuration();

        this.catalog = new HadoopCatalog(configuration, warehousePath);
    }

    public Table createTable(String namespace, String tableName, Schema schema) {

        TableIdentifier tableIdentifier = TableIdentifier.of(namespace, tableName);

        if (!isTableExists(namespace, tableName)) {
            Map<String, String> tableProperties = new HashMap();
            tableProperties.put(TableProperties.FORMAT_VERSION, "2");
            Transaction transaction = catalog.newCreateTableTransaction(tableIdentifier, schema, PartitionSpec.unpartitioned(), tableProperties);
            transaction.commitTransaction();
        }
        return this.loadTable(namespace, tableName);
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

    public List<Record> loadDataRecord(Table table, Predicate filterExpression, String... columns) {
        IcebergGenerics.ScanBuilder read = IcebergGenerics.read(table);
        IcebergGenerics.ScanBuilder scaner = read.where(filterExpression);
        CloseableIterable<Record> records;
        if (columns.length == 0) {
            records = scaner
                    .build();
        } else {
            records = scaner
                    .select(columns)
                    .build();
        }

        return Lists.newArrayList(records.iterator());
    }

    public CloseableIterator<Record> listCowTableRecords() {
        Table cowTable = loadTable(icebergNamespace, cowTableName);
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(cowTable);

        return scanBuilder.select("ss_sold_date_sk",
                "ss_sold_time_sk",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_cdemo_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_store_sk",
                "ss_promo_sk",
                "ss_ticket_number",
                "ss_quantity",
                "ss_wholesale_cost",
                "ss_list_price",
                "ss_sales_price",
                "ss_ext_discount_amt",
                "ss_ext_sales_price",
                "ss_ext_wholesale_cost",
                "ss_ext_list_price",
                "ss_ext_tax",
                "ss_coupon_amt",
                "ss_net_paid",
                "ss_net_paid_inc_tax",
                "ss_net_profit").build().iterator();
    }

    public CloseableIterator<Record> listMorTableRecords() {
        Table morTable = loadTable(icebergNamespace, morTableName);

        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(morTable);

        return scanBuilder.select("ss_sold_date_sk",
                "ss_sold_time_sk",
                "ss_item_sk",
                "ss_customer_sk",
                "ss_cdemo_sk",
                "ss_hdemo_sk",
                "ss_addr_sk",
                "ss_store_sk",
                "ss_promo_sk",
                "ss_ticket_number",
                "ss_quantity",
                "ss_wholesale_cost",
                "ss_list_price",
                "ss_sales_price",
                "ss_ext_discount_amt",
                "ss_ext_sales_price",
                "ss_ext_wholesale_cost",
                "ss_ext_list_price",
                "ss_ext_tax",
                "ss_coupon_amt",
                "ss_net_paid",
                "ss_net_paid_inc_tax",
                "ss_net_profit").build().iterator();
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

    public void writeDataToCowTable(List<GenericRecord> records) throws IOException {
        Table cowTable = loadTable(icebergNamespace, cowTableName);
        writeData(records, cowTable);
    }

    private static void writeData(List<GenericRecord> records, Table table) throws IOException {
        DataFile dataFile = writeDataFile(records, table);
        table.newAppend().appendFile(dataFile).commit();
    }

    private static DataFile writeDataFile(List<GenericRecord> records, Table table) throws IOException {
        String filepath = table.location() + "/" + UUID.randomUUID();
        OutputFile file = table.io().newOutputFile(filepath);

        DataWriter<GenericRecord> dataWriter =
                Parquet.writeData(file)
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();

        try {
            for (GenericRecord record : records) {
                dataWriter.write(record);
            }
        } finally {
            dataWriter.close();
        }

        DataFile dataFile = dataWriter.toDataFile();
        return dataFile;
    }

    public void writeDataToMorTable(List<GenericRecord> records) throws IOException {
        Table cowTable = loadTable(icebergNamespace, morTableName);
        writeData(records, cowTable);
    }


    public void updateCowRecords(List<GenericRecord> records) throws IOException {
        Table cowTable = loadTable(icebergNamespace, cowTableName);
        DataFile dataFile = writeDataFile(records, cowTable);
        cowTable.newOverwrite().addFile(dataFile).commit();
    }

    public void deleteCowRecords(List<GenericRecord> records) {
        Table cowTable = loadTable(icebergNamespace, cowTableName);
        cowTable.newDelete().deleteFromRowFilter(Expressions.lessThan("ss_sold_date_sk", 245136300)).commit();
    }

    public void deleteByExpression(Predicate deleteExpression) throws IOException {
        Table cowTable = loadTable(icebergNamespace, cowTableName);
        String deletePath = cowTable.location() + "/" + UUID.randomUUID();

        OutputFile out = cowTable.io().newOutputFile(deletePath);

        PositionDeleteWriter<Record> positionDeleteWriter = Parquet.writeDeletes(out)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .overwrite()
                .rowSchema(cowTable.schema())
                .withSpec(PartitionSpec.unpartitioned())
                .buildPositionWriter();

        PositionDelete<Record> positionDelete = PositionDelete.create();

        List<IcebergDeleteRecord> deleteRecords = getDeleteRecords(deleteExpression, cowTable);

        deleteRecords.forEach(deleteRecord -> {
            Record record = deleteRecord.getRecord();
            Long position = deleteRecord.getPosition();
            String dataFilePath = deleteRecord.getDataFile().path().toString();
            positionDeleteWriter.write(positionDelete.set(dataFilePath, position, record));
        });

        positionDeleteWriter.close();
        DeleteFile deleteFile = positionDeleteWriter.toDeleteFile();

        cowTable.newRowDelta().addDeletes(deleteFile).commit();
    }

    private List<IcebergDeleteRecord> getDeleteRecords(Predicate deleteExpression, Table cowTable) {
        List<Record> deleteRecords = loadDataRecord(cowTable, deleteExpression);
        TableScan scan = cowTable.newScan().filter(deleteExpression);

        CloseableIterable<CombinedScanTask> result = scan.planTasks();

        Iterable<FileScanTask> fileTasks = Iterables.concat(Iterables.transform(result, CombinedScanTask::files));

        DataFile dataFile;

        List<IcebergDeleteRecord> deleteResult = new ArrayList<>();

        for (FileScanTask fileTask : fileTasks) {
            dataFile = fileTask.file();

            String s = dataFile.path().toString();
            List<Record> records = readParquetDataFile(cowTable, s);

            DataFile finalDataFile = dataFile;
            for (int index = 0; index < records.size(); index++) {
                if(deleteRecords.contains(records.get(index))){
                    deleteResult.add(new IcebergDeleteRecord(records.get(index), finalDataFile, index));
                }
            }
        }

        return deleteResult;

    }

    private List<Record> readParquetDataFile(Table cowTable, String filePath) {
        OutputFile outputFile = cowTable.io().newOutputFile(filePath);

        Schema schema = cowTable.schema();

        List<Record> records = null;

        try (CloseableIterable<Record> build = Parquet.read(outputFile.toInputFile())
                .project(schema)
                .createReaderFunc(fileSchema -> GenericParquetReaders.buildReader(schema, fileSchema))
                .build()) {

            records = Lists.newArrayList(build.iterator());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return records;
    }
}
