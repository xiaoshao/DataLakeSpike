package com.zwshao.flink.read;

import com.zwshao.flink.utils.FlinkUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class HudiReadIncrementApplication {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = FlinkUtils.createExecutionEnv();

    }
}
