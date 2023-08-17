package com.zwshao.sdk.write.mor;

import com.zwshao.sdk.utils.CSVRecordParse;
import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.data.GenericRecord;

import java.io.IOException;
import java.util.List;

import static com.zwshao.sdk.utils.IcebergConst.*;

public class IcebergSDKWriteApplication {


    public static void main(String[] args) throws IOException {

        IcebergTableOperation operation = new IcebergTableOperation();

        operation.createTable(icebergNamespace, morTableName, schema);

        System.out.println(operation.loadTable(icebergNamespace, morTableName).location());
        CSVRecordParse csvRecordParse = new CSVRecordParse("/Users/shaozengwei/projects/data/input/store_sales.dat");
        List<GenericRecord> records = csvRecordParse.nextBatch(1000);
        int count = 0;

        while (records.size() > 0 && count++ < 3) {
            operation.writeDataToMorTable(records);
        }
    }
}
