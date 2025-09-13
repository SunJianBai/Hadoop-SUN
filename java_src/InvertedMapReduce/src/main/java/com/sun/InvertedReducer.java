package com.sun;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Reducer 将 Combiner/Mapper 输出聚合为最终的反向索引，并直接写入 HBase。
 *
 * 设计说明：
 * - Reducer 接收 key 为单词 (word)，values 为多个 "filename:count" 字符串
 * - 将这些文件列表拼接为以分号分隔的字符串，并使用 HBase 的 Put 将结果写到表中
 * - 使用 HBase 写入可以实现实时查询与存储的结合，便于后续通过 HBase 直接检索倒排索引
 */
public class InvertedReducer extends TableReducer<Text,Text, ImmutableBytesWritable> {
    private static final Text result = new Text();
    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
        StringBuilder fileList = new StringBuilder();
        for (Text value:values){
            fileList.append(value.toString()).append(";");
        }
        result.set(fileList.toString());
        // 使用单词作为 rowkey，将所有文件:count 写入 info:index 列
        Put put = new Put(key.toString().getBytes());
        put.addColumn("info".getBytes(), "index".getBytes(), result.toString().getBytes());
        // TableReducer 的 context.write 将把 Put 写入指定的 HBase 表
        context.write(null, put);
    }
}
