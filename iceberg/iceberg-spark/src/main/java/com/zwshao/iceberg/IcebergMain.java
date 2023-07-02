package com.zwshao.iceberg;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;

import java.util.List;

public class IcebergMain {
    public static void main(String[] args) {
        IcebergTableOperation operation = new IcebergTableOperation();

        Schema schema = new Schema(
                Types.NestedField.required(1, "level", Types.StringType.get()),
                Types.NestedField.required(2, "event_time", Types.TimestampType.withZone()),
                Types.NestedField.required(3, "message_type", Types.StringType.get())
        );

        String namespace = "zw";
        String tableName = "first_iceberg";
        operation.createTable(namespace, tableName, schema, PartitionSpec.builderFor(schema).hour("event_time").identity("level").build());

        Table firstIceberg = operation.loadTable(namespace, tableName);

        System.out.println(firstIceberg.location());

        // 用户spark 分片读写
        CloseableIterable<CombinedScanTask> combinedScanTasks = operation.loadData(firstIceberg, Expressions.equal("level", "10"), "level", "event_time");

        CloseableIterator<CombinedScanTask> iterator = combinedScanTasks.iterator();
        while (iterator.hasNext()) {
            System.out.println(iterator.next());
        }

        List<Record> records = operation.loadDataRecord(firstIceberg, Expressions.equal("level", "50"), "level", "event_time", "message_type");

        for (Record record : records) {
            System.out.println(record);
        }
    }
}
