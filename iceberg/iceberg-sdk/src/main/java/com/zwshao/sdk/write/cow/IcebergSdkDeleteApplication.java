package com.zwshao.sdk.write.cow;

import com.zwshao.sdk.utils.CSVRecordParse;
import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.expressions.Expressions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

public class IcebergSdkDeleteApplication {
    public static void main(String[] args) throws IOException {
        IcebergTableOperation operation = new IcebergTableOperation();

//        CSVRecordParse csvRecordParse = null;
//
//        try {
//            csvRecordParse = new CSVRecordParse("/Users/shaozengwei/projects/data/input/store_sales.dat");

//            List<GenericRecord> records = csvRecordParse.nextUpdateBatch(1000);

//            while (records.size() > 0) {
                operation.deleteByExpression(Expressions.greaterThan("ss_sold_date_sk", 1000));
//                records = csvRecordParse.nextUpdateBatch(1000);
//            }
//        } finally {
//            if (csvRecordParse != null) {
//                csvRecordParse.close();
//            }
//        }

    }
}
