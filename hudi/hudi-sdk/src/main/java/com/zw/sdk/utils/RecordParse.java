package com.zw.sdk.utils;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.io.*;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static com.zw.sdk.utils.SDKConst.hudi_schema;

public class RecordParse {

    private BufferedReader reader;

    public RecordParse(Path csvPath) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(new File(new File(csvPath.toUri()).toURI())));
    }

    public List<HoodieRecord> next(int number) throws IOException {
        List<HoodieRecord> result = new ArrayList<>();
        int count = 0;
        String line;
        while ((line = reader.readLine()) != null && count < number) {
            HoodieRecord records = parse(line);
            if (records != null) {
                result.add(records);
                count++;
            }
        }

        return result;
    }


    public List<HoodieRecord> next4Update(int number) throws IOException {
        List<HoodieRecord> result = new ArrayList<>();
        int count = 0;
        String line;
        while ((line = reader.readLine()) != null && count < number) {
            HoodieRecord records = parse4Update(line);
            if (records != null) {
                result.add(records);
                count++;
            }
        }

        return result;
    }

    public void close() throws IOException {
        if (reader != null) {
            this.reader.close();
        }
    }

    public HoodieRecord parse(String csvLine) {
        GenericRecord value = new GenericData.Record(hudi_schema);

        String[] items = csvLine.split("\\|");


        if (items.length < 23) {
            return null;
        }
        value.put("ss_sold_date_sk", Integer.valueOf(withDefault(items[0])));
        value.put("ss_sold_time_sk", Integer.valueOf(withDefault(items[1])));
        value.put("ss_item_sk", Integer.valueOf(withDefault(items[2])));
        value.put("ss_customer_sk", Integer.valueOf(withDefault(items[3])));
        value.put("ss_cdemo_sk", Integer.valueOf(withDefault(items[4])));
        value.put("ss_hdemo_sk", Integer.valueOf(withDefault(items[5])));
        value.put("ss_addr_sk", Integer.valueOf(withDefault(items[6])));
        value.put("ss_store_sk", Integer.valueOf(withDefault(items[7])));
        value.put("ss_promo_sk", Integer.valueOf(withDefault(items[8])));
        value.put("ss_ticket_number", Integer.valueOf(withDefault(items[9])));
        value.put("ss_quantity", Integer.valueOf(withDefault(items[10])));
        value.put("ss_wholesale_cost", Double.valueOf(withDefault(items[11])));
        value.put("ss_list_price", Double.valueOf(withDefault(items[12])));
        value.put("ss_sales_price", Double.valueOf(withDefault(items[13])));
        value.put("ss_ext_discount_amt", Double.valueOf(withDefault(items[14])));
        value.put("ss_ext_sales_price", Double.valueOf(withDefault(items[15])));
        value.put("ss_ext_wholesale_cost", Double.valueOf(withDefault(items[16])));
        value.put("ss_ext_list_price", Double.valueOf(withDefault(items[17])));
        value.put("ss_ext_tax", Double.valueOf(withDefault(items[18])));
        value.put("ss_coupon_amt", Double.valueOf(withDefault(items[19])));
        value.put("ss_net_paid", Double.valueOf(withDefault(items[20])));
        value.put("ss_net_paid_inc_tax", Double.valueOf(withDefault(items[21])));
        value.put("ss_net_profit", Double.valueOf(withDefault(items[22])));

        HoodieKey key = buildHoodieKey(items);

        return new HoodieAvroRecord(key, new HoodieAvroPayload(Option.of(value)));
    }


    private static HoodieKey buildHoodieKey(String[] items) {
        StringBuilder sb = new StringBuilder();
        for (int index = 0; index <= 8; index++) {
            sb.append(items[index]).append(",");
        }

        return new HoodieKey(sb.toString(), "partitionPath");
    }

    public HoodieRecord parse4Update(String csvLine) {
        GenericRecord value = new GenericData.Record(hudi_schema);

        String[] items = csvLine.split("\\|");


        if (items.length < 23) {
            return null;
        }
        value.put("ss_sold_date_sk", Integer.valueOf(withDefault(items[0])));
        value.put("ss_sold_time_sk", Integer.valueOf(withDefault(items[1])));
        value.put("ss_item_sk", Integer.valueOf(withDefault(items[2])));
        value.put("ss_customer_sk", Integer.valueOf(withDefault(items[3])));
        value.put("ss_cdemo_sk", Integer.valueOf(withDefault(items[4])));
        value.put("ss_hdemo_sk", Integer.valueOf(withDefault(items[5])));
        value.put("ss_addr_sk", Integer.valueOf(withDefault(items[6])));
        value.put("ss_store_sk", Integer.valueOf(withDefault(items[7])));
        value.put("ss_promo_sk", Integer.valueOf(withDefault(items[8])));
        value.put("ss_ticket_number", Integer.valueOf(withDefault(items[9])));
        value.put("ss_quantity", Integer.valueOf(withDefault(items[10])));
        value.put("ss_wholesale_cost", Double.valueOf(withDefault(items[11])));
        value.put("ss_list_price", Double.valueOf(withDefault(items[12])));
        value.put("ss_sales_price", Double.valueOf(withDefault(items[13])));
        value.put("ss_ext_discount_amt", Double.valueOf(withDefault(items[14])));
        value.put("ss_ext_sales_price", Double.valueOf(withDefault(items[15])));
        value.put("ss_ext_wholesale_cost", Double.valueOf(withDefault(items[16])));
        value.put("ss_ext_list_price", Double.valueOf(withDefault(items[17])));
        value.put("ss_ext_tax", Double.valueOf(withDefault(items[18])));
        value.put("ss_coupon_amt", Double.valueOf(withDefault(items[19])));
        value.put("ss_net_paid", Double.valueOf(withDefault(items[20])));
        value.put("ss_net_paid_inc_tax", Double.valueOf(withDefault(items[21])));
        value.put("ss_net_profit", 10000.0);

        HoodieKey key = buildHoodieKey(items);

        return new HoodieAvroRecord(key, new HoodieAvroPayload(Option.of(value)));
    }

    private static String withDefault(String item) {
        if (StringUtils.isNullOrEmpty(item)) {
            return "0";
        }
        return item;
    }
}
