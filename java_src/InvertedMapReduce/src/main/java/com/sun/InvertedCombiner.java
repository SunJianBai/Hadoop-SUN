package com.sun;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner 用于在 Map 端对相同的中间 key 做本地合并，减少经网络传输的数据量。
 *
 * 设计说明：
 * - 输入 key 形如 "word:filename"，values 为若干个 "1" 字符串（表示出现次数），Combiner 对这些进行求和
 * - 之后把 key 重写为 "word"，value 写为 "filename:count"，便于 Reducer 汇总不同文件的出现情况
 * - 这样做能在 Map 端把同一文件的重复计数合并为单条记录，从而减少 Shuffle 流量
 */
public class InvertedCombiner extends Reducer<Text,Text,Text,Text> {
    private final Text valueInfo = new Text();

    @Override
    protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
        int sum = 0;
        for (Text value:values){
            sum += Integer.parseInt(value.toString());
        }
        int fileNameIndex = key.toString().indexOf(":");
        // 将 value 设为 "filename:count"
        valueInfo.set(key.toString().substring(fileNameIndex+1)+":"+sum);
        // 将 key 设为纯单词 "word"
        key.set(key.toString().substring(0,fileNameIndex));
        // 输出格式为 <word, filename:count>
        context.write(key,valueInfo);
    }
}
