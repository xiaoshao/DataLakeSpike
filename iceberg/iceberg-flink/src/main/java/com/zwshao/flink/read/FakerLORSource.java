package com.zwshao.flink.read;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Map;

public class FakerLORSource implements SourceFunction<Map> {
    @Override
    public void run(SourceContext<Map> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
