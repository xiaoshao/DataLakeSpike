package com.zwshao.flink.read;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;

import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;
import org.apache.iceberg.types.Types;

import java.util.Map;

public class FlinkReadApplication {
    public static void main(String[] args) throws Exception {

        String property = System.getProperty("java.class.path");
        System.out.println(property);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(1000);

        FakerLORSource source = new FakerLORSource();

        DataStream<Row> stream = env.addSource(source)
                .returns(TypeInformation.of(Map.class))
                .map(s -> {
                    Row row = new Row(3);
                    row.setField(0, s.get("character"));
                    row.setField(1, s.get("location"));
                    row.setField(2, s.get("event_time"));
                    return row;
                });

        Configuration hadoopConf = new Configuration();
        TableLoader tableLoader = TableLoader.fromHadoopTable("hdfs://localhost:9000/warehouse/path", hadoopConf);

        Schema schema = new Schema(
                Types.NestedField.required(1, "character", Types.StringType.get()),
                Types.NestedField.required(2, "location", Types.StringType.get()),
                Types.NestedField.required(3, "event_time", Types.TimestampType.withZone()));
        DataStreamSink<RowData> sink = FlinkSink.forRow(stream, FlinkSchemaUtil.toSchema(schema))
                .tableLoader(tableLoader)
                .distributionMode(DistributionMode.HASH)
                .overwrite(true)
                .writeParallelism(1).build();


        env.execute("env");
    }
}
