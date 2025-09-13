package com.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseReaderQuery {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: HBaseReaderQuery <tableName> <rowKey>");
            System.exit(1);
        }
        String tableName = args[0];
        String rowKey = args[1];

        Configuration conf = HBaseConfiguration.create();

        try (Connection conn = ConnectionFactory.createConnection(conf);
             Table table = conn.getTable(TableName.valueOf(tableName))) {

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes("info"), Bytes.toBytes("index"));

            Result r = table.get(get);
            if (r.isEmpty()) {
                System.out.println("RowKey = " + rowKey + " not found in " + tableName);
            } else {
                byte[] v = r.getValue(Bytes.toBytes("info"), Bytes.toBytes("index"));
                System.out.println("RowKey = " + rowKey);
                System.out.println("Value  = " + Bytes.toString(v));
            }
        }
    }
}
