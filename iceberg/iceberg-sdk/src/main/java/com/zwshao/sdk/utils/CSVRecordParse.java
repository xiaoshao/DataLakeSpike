package com.zwshao.sdk.utils;

import com.google.common.collect.ImmutableList;
import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.data.GenericRecord;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.zwshao.sdk.utils.IcebergConst.schema;

public class CSVRecordParse {

    private BufferedReader reader;
    private GenericRecord record = GenericRecord.create(schema);

    public CSVRecordParse(String csvPath) throws FileNotFoundException {
        this.reader = new BufferedReader(new FileReader(csvPath));
    }

    public List<GenericRecord> nextBatch(int count) throws IOException {
        ImmutableList.Builder<GenericRecord> builder = ImmutableList.builder();
        String line;
        int number = 0;
        while ((line = reader.readLine()) != null && number++ < count) {
            GenericRecord record = parse(line);
            if (record != null) {
                builder.add(record);
            }
        }
        return builder.build();
    }

    private GenericRecord parse(String csvLine) {
        Map<String, Object> data = new HashMap<String, Object>();

        String[] items = csvLine.split("\\|");

        if (items.length < 23) {
            return null;
        }

        data.put("ss_sold_date_sk", Integer.valueOf(withDefault(items[0])));
        data.put("ss_sold_time_sk", Integer.valueOf(withDefault(items[1])));
        data.put("ss_item_sk", Integer.valueOf(withDefault(items[2])));
        data.put("ss_customer_sk", Integer.valueOf(withDefault(items[3])));
        data.put("ss_cdemo_sk", Integer.valueOf(withDefault(items[4])));
        data.put("ss_hdemo_sk", Integer.valueOf(withDefault(items[5])));
        data.put("ss_addr_sk", Integer.valueOf(withDefault(items[6])));
        data.put("ss_store_sk", Integer.valueOf(withDefault(items[7])));
        data.put("ss_promo_sk", Integer.valueOf(withDefault(items[8])));
        data.put("ss_ticket_number", Integer.valueOf(withDefault(items[9])));
        data.put("ss_quantity", Integer.valueOf(withDefault(items[10])));
        data.put("ss_wholesale_cost", Double.valueOf(withDefault(items[11])));
        data.put("ss_list_price", Double.valueOf(withDefault(items[12])));
        data.put("ss_sales_price", Double.valueOf(withDefault(items[13])));
        data.put("ss_ext_discount_amt", Double.valueOf(withDefault(items[14])));
        data.put("ss_ext_sales_price", Double.valueOf(withDefault(items[15])));
        data.put("ss_ext_wholesale_cost", Double.valueOf(withDefault(items[16])));
        data.put("ss_ext_list_price", Double.valueOf(withDefault(items[17])));
        data.put("ss_ext_tax", Double.valueOf(withDefault(items[18])));
        data.put("ss_coupon_amt", Double.valueOf(withDefault(items[19])));
        data.put("ss_net_paid", Double.valueOf(withDefault(items[20])));
        data.put("ss_net_paid_inc_tax", Double.valueOf(withDefault(items[21])));
        data.put("ss_net_profit", Double.valueOf(withDefault(items[22])));


        return record.copy(data);
    }


    public void close() throws IOException {
        if (reader != null) {
            reader.close();
        }
    }

    private static String withDefault(String item) {
        if (StringUtils.isAllEmpty(item)) {
            return "0";
        }
        return item;
    }
}
