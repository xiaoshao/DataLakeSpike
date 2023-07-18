package com.zwshao.flink.util;

import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkUtils {
    public static StreamExecutionEnvironment createExecutionEnv() {
        org.apache.flink.configuration.Configuration configuration = new org.apache.flink.configuration.Configuration();
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        env.enableCheckpointing(1000);

        return env;
    }
}
