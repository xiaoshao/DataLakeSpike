package com.zwshao.sdk.write.bean;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.data.Record;

public class IcebergDeleteRecord {
    private Record record;
    private DataFile dataFile;

    private long position;

    public IcebergDeleteRecord(Record record, DataFile dataFile, long position) {
        this.record = record;
        this.dataFile = dataFile;
        this.position = position;
    }
}
