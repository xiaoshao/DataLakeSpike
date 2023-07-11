package com.zwshao.flink.read;

import org.apache.commons.net.ntp.TimeStamp;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.HashMap;
import java.util.Map;

public class FakerLORSource implements ParallelSourceFunction<Map> {

    boolean isRunning = true;

    long count = 0;

    @Override
    public void run(SourceContext<Map> sourceContext) throws InterruptedException {
        while (isRunning) {

            count++;
            Map t = new HashMap();
            t.put("character", "first_character" + count);
            t.put("location", "first_location" + count);
            t.put("event_time", "10000" + count);
            sourceContext.collect(t);

            Thread.sleep(20000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
