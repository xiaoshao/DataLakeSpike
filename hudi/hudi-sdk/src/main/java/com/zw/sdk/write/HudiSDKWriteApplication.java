package com.zw.sdk.write;

import com.zw.sdk.utils.RecordParse;
import com.zw.sdk.utils.SDKConst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class HudiSDKWriteApplication {

    public static void main(String[] args) throws IOException {
        WriteSupport
        SDKConst.initCopyOnWriteHudiTable(new Configuration());
        HoodieWriteConfig config = HoodieWriteConfig.newBuilder()
                .withPath(SDKConst.getCowHudiTablePath())
                .withSchema(SDKConst.hudi_schema.toString(true))
                .withParallelism(2, 2)
                .forTable(SDKConst.cow_table_name)
                .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
                .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(20, 30).build())
                .build();

        HoodieJavaWriteClient client = null;
        RecordParse recordParse = null;

        try {
            client = new HoodieJavaWriteClient(new HoodieJavaEngineContext(new Configuration()), config);

            String startCommitTime = client.startCommit();
            recordParse = new RecordParse(Paths.get("/Users/shaozengwei/projects/data/store_sales/store_sales.dat"));
            List<HoodieRecord> next = recordParse.next(1000);
            int count = 0;
            while (next.size() > 0 && count++ < 3) {
                client.insert(next, startCommitTime);
                next = recordParse.next(1000);
            }
        } finally {
            if (client != null) {
                client.close();
            }
            recordParse.close();
        }
    }
}
