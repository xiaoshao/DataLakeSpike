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

    public Record getRecord() {
        return record;
    }

    public void setRecord(Record record) {
        this.record = record;
    }

    public DataFile getDataFile() {
        return dataFile;
    }

    public void setDataFile(DataFile dataFile) {
        this.dataFile = dataFile;
    }

    public long getPosition() {
        return position;
    }

    public void setPosition(long position) {
        this.position = position;
    }
}
