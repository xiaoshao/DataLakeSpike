package com.zwshao.flink.read;

import com.google.common.collect.ImmutableMap;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;

import java.util.Map;

public class FlinkReadApplication {
    public static void main(String[] args) throws Exception {

        String property = System.getProperty("java.class.path");
        System.out.println(property);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);

        FakerLORSource source = new FakerLORSource();

        DataStream<RowData> stream = env.addSource(source)
                .returns(TypeInformation.of(Map.class))
                .map(s -> {
                    GenericRowData row = new GenericRowData(3);
                    row.setField(0, s.get("character"));
                    row.setField(1, s.get("location"));
                    row.setField(2, s.get("event_time"));
                    return row;
                });

        Schema schema = new Schema(
                Types.NestedField.required(1, "character", Types.StringType.get()),
                Types.NestedField.required(2, "location", Types.StringType.get()),
                Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));

        PartitionSpec spec = PartitionSpec.builderFor(schema).identity("character").build();
        Map<String, String> props =
                ImmutableMap.of(TableProperties.DEFAULT_FILE_FORMAT, FileFormat.PARQUET.name());
        Configuration hadoopConf = new Configuration();
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, "hdfs://localhost:9000/srv/iceberg");
        TableIdentifier tableName = TableIdentifier.of("zw", "second_iceberg");
        Table table = null;
        if (catalog.tableExists(tableName)) {
            table = catalog.loadTable(tableName);
        } else {
            table = catalog.createTable(tableName, schema, spec, props);
        }


        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://localhost:9000/srv/iceberg/zw/second_iceberg", hadoopConf);

        DataStreamSink<RowData> sink = FlinkSink.forRowData(stream)
                .table(table)
                .tableLoader(tableLoader)
                .distributionMode(DistributionMode.HASH)
                .overwrite(true)
                .writeParallelism(1).build();


        env.execute("env");
    }
}
