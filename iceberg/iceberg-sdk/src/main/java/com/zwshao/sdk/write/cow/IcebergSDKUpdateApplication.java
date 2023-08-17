package com.zwshao.sdk.write.cow;

import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;

import java.util.List;

import static com.zwshao.sdk.utils.IcebergConst.*;

public class IcebergSDKUpdateApplication {
    public static void main(String[] args) {
        IcebergTableOperation operation = new IcebergTableOperation();

        operation.createTable(icebergNamespace, cowTableName, schema);

        Table firstIceberg = operation.loadTable(icebergNamespace, cowTableName);

        System.out.println(firstIceberg.location());

        List<Record> records = operation.loadDataRecord(firstIceberg, Expressions.equal("level", "50"), "level", "event_time", "message_type");

        for (Record record : records) {
            System.out.println(record);
        }
    }
}
