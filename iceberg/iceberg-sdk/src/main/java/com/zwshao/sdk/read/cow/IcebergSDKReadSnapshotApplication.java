package com.zwshao.sdk.read.cow;

import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;

public class IcebergSDKReadSnapshotApplication {
    public static void main(String[] args) {
        IcebergTableOperation operation = new IcebergTableOperation();

        CloseableIterator<Record> recordsIterator = operation.listCowTableRecords();

        int count = 0;
        while (recordsIterator.hasNext()){
            System.out.println(recordsIterator.next());
            count ++;
        }
        System.out.println("the total count is " + count);
    }
}
