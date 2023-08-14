package com.zw.sdk.write;

import com.zw.sdk.utils.SDKConst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class HudiSDKWriteApplication {
    public static void main(String[] args) throws IOException {
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
        try {

            client = new HoodieJavaWriteClient(new HoodieJavaEngineContext(new Configuration()), config);

            String startCommitTime = client.startCommit();

            client.insert(new ArrayList<HoodieRecord>(), startCommitTime);
        } finally {
            if (client != null) {
                client.close();
            }
        }
    }
}
