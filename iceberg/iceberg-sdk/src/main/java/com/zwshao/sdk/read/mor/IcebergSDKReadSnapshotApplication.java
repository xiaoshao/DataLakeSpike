package com.zwshao.sdk.read.mor;

import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;

public class IcebergSDKReadSnapshotApplication {
    public static void main(String[] args) {
        IcebergTableOperation operation = new IcebergTableOperation();

        CloseableIterator<Record> recordsIterator = operation.listMorTableRecords();

        while (recordsIterator.hasNext()){
            System.out.println(recordsIterator.next());
        }
    }
}
