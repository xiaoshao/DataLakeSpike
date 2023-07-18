package com.zwshao.flink.read;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

import static com.zwshao.flink.FlinkConst.ICEBERG_TABLE_LOCATION;
import static com.zwshao.flink.util.FlinkUtils.createExecutionEnv;

public class FlinkReadIncrementApplication {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = createExecutionEnv();

        Configuration hadoopConf = new Configuration();

        TableLoader tableLoader = TableLoader.fromHadoopTable(ICEBERG_TABLE_LOCATION, hadoopConf);

        DataStream<RowData> batch = FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .startSnapshotId(2L)
                .endSnapshotId(3L)
                .streaming(false)
                .build();

        batch.print();

        env.execute(" execute FlinkReadSnapshotApplication");
    }
}
