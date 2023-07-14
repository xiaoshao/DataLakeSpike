package com.zwshao.flink;

import java.io.File;

public class FlinkConst {

    public static String ICEBERG_LOCATION = "hdfs://localhost:9000/srv/iceberg";

    public static String ICEBERG_CATALOG = "zw";

    public static String ICEBERG_TABLE = "iceberg_table";

    public static String ICEBERG_TABLE_LOCATION = String.join(File.separator, ICEBERG_LOCATION, ICEBERG_CATALOG, ICEBERG_TABLE);
}
