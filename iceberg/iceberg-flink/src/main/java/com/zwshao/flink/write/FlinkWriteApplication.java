package com.zwshao.flink.write;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.zwshao.flink.FlinkConst;
import com.zwshao.flink.read.FakerLORSource;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.*;

public class FlinkWriteApplication {
    public static void main(String[] args) throws Exception {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.enableCheckpointing(1000);

        FakerLORSource source = new FakerLORSource();

        DataStream<RowData> stream = env.addSource(source)
                .map(s -> {
                    GenericRowData row = new GenericRowData(3);
                    row.setField(0, StringData.fromString((String) s.get("character")));
                    row.setField(1, StringData.fromString((String) s.get("location")));
                    row.setField(2, StringData.fromString((String) s.get("event_time")));
                    return row;
                });

        Schema schema = new Schema(Lists.newArrayList(Types.NestedField.required(1, "character", Types.StringType.get()),
                Types.NestedField.required(2, "location", Types.StringType.get()),
                Types.NestedField.required(3, "event_time", Types.StringType.get())),
                Sets.newHashSet(1));

        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("character").build();

        Map<String, String> props =
                ImmutableMap.of(
                        TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name(),
                        TableProperties.UPSERT_ENABLED, "true"
                );
        Configuration hadoopConf = new Configuration();

        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, FlinkConst.ICEBERG_LOCATION);
        TableIdentifier tableName = TableIdentifier.of(FlinkConst.ICEBERG_CATALOG, FlinkConst.ICEBERG_TABLE);

        Table table = null;
        if (!catalog.tableExists(tableName)) {
            table = catalog.createTable(tableName, schema, spec, props);
        } else {
            table = catalog.loadTable(tableName);
        }

        TableLoader tableLoader = TableLoader.fromHadoopTable(FlinkConst.ICEBERG_TABLE_LOCATION, hadoopConf);

        FlinkSink.forRowData(stream)
                .table(table)
                .tableLoader(tableLoader)
                .overwrite(false)
                .writeParallelism(1).upsert(true).append();

        env.execute("env");
    }
}
