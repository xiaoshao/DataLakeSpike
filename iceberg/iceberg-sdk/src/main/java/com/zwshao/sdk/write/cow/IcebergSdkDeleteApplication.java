package com.zwshao.sdk.write.cow;

import com.zwshao.sdk.utils.IcebergTableOperation;
import org.apache.iceberg.expressions.Expressions;

import java.io.IOException;

public class IcebergSdkDeleteApplication {
    public static void main(String[] args) throws IOException {
        IcebergTableOperation operation = new IcebergTableOperation();

        operation.deleteByExpression(Expressions.greaterThan("ss_sold_date_sk", 1000));
    }
}
