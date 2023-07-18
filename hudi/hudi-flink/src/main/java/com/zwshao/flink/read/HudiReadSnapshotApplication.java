package com.zwshao.flink.read;

import com.zwshao.flink.utils.FlinkUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HudiReadSnapshotApplication {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = FlinkUtils.createExecutionEnv();

    }
}
