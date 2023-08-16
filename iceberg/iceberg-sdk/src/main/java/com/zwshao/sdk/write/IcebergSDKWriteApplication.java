package com.zwshao.sdk.write;

import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.types.Types;

import java.util.List;

import static com.zwshao.sdk.utils.IcebergConst.*;

public class IcebergSDKWriteApplication {



    public static void main(String[] args) {
        IcebergTableOperation operation = new IcebergTableOperation();

        operation.createTable(icebergNamespace, cowTableName, schema, PartitionSpec.builderFor(schema).hour("event_time").identity("level").build());

        Table firstIceberg = operation.loadTable(icebergNamespace, cowTableName);

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
