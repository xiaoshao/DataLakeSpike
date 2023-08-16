package com.zw.sdk.write.mor;

import com.zw.sdk.utils.RecordParse;
import com.zw.sdk.utils.SDKConst;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class HudiSDKWriteApplication {

    public static void main(String[] args) throws IOException {
        SDKConst.initMORHudiTable(new Configuration());
        HoodieWriteConfig config = SDKConst.createHoodieWriteConfig(SDKConst.mor_table_name);

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
                startCommitTime = client.startCommit();
            }
        } finally {
            if (client != null) {
                client.close();
            }
            recordParse.close();
        }
    }

}
