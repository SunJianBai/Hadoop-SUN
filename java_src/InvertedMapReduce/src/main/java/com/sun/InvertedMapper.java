package com.sun;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;


/**
 * Mapper 负责将每行输入转换为 (word:file, 1) 形式的中间键值对。
 *
 * 设计说明：
 * - 输入假定为每行以空格分隔，第一列可能是句子编号或其它元信息，后续为句子中的单词集合
 * - Map 输出的 key 使用 "word:filename" 格式，value 初始为字符串 "1"，表示该单词在该句子中出现一次
 * - 这样做的好处是：Combiner 可以在 map 端聚合同一文件内该词的出现次数，从而显著减少要传输给 Reducer 的数据量
 */
public class InvertedMapper extends Mapper<LongWritable, Text, Text, Text>
{
    // keyInfo 存储 "word:filename"，valueInfo 初始为 "1"
    private final Text keyInfo = new Text();
    private final Text valueInfo = new Text("1");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {
        // 把整行按空格拆分。第一部分可能是句子编号，后续为单词
        String[] orderedSentences = value.toString().split(" ");

        // 获取当前输入分片对应的文件名，便于构建 file-level 索引
        FileSplit filesplit = (FileSplit) context.getInputSplit();
        String filename = filesplit.getPath().getName();

        // 从第二列开始是句子的单词（按代码原逻辑）
        String[] sentences = Arrays.copyOfRange(orderedSentences, 1, orderedSentences.length);

        // 对句子内每个单词输出 <word:filename, 1>
        // 这样 Combiner/Reducer 可以按 word 聚合并输出 file:count
        for (String word : sentences)
        {
            keyInfo.set(word + ":" + filename);
            context.write(keyInfo, valueInfo);
        }
    }
}
