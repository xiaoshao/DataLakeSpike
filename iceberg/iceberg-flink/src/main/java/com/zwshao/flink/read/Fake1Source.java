package com.zwshao.flink.read;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.HashMap;
import java.util.Map;

public class Fake1Source implements ParallelSourceFunction<String> {

    boolean isRunning = true;

    long count = 0;

    @Override
    public void run(SourceContext<String> sourceContext) throws InterruptedException {
        while (isRunning) {

            count++;

            sourceContext.collect("1,2,3,4");

            Thread.sleep(20000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
