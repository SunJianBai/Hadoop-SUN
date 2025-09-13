package com.sun;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Driver 程序：MapReduce 作业入口。
 *
 * 设计说明：
 * - 该类负责组装并提交一个 Hadoop MapReduce 作业：设置 Mapper/Combiner/Reducer、输入路径、以及 HBase 输出表
 * - 将 Reducer 配置为 TableReducer 可直接写入 HBase，从而避免额外的导入步骤（性能与工程便利性）
 * - 在 Configuration 中显式设置 fs.defaultFS、yarn.resourcemanager.hostname、hbase.zookeeper.quorum，
 *   这使得该驱动能够在不同环境（本地伪分布或真实集群）之间更容易切换，不依赖于外部 shell 环境变量
 */
public class Driver {
    public static void main(String[] args) throws ClassNotFoundException, IOException,InterruptedException {
        Configuration conf = new Configuration();
        // HDFS NameNode 地址——明确写出可以避免依赖外部环境变量而导致的连接错误
        conf.set("fs.defaultFS","hdfs://master:9000");
        // YARN ResourceManager 地址
        conf.set("yarn.resourcemanager.hostname","master");
        // HBase 的 ZooKeeper quorum，Reducer 会直接向 HBase 写入结果
        conf.set("hbase.zookeeper.quorum","master,worker1,worker2,worker3");

        // 创建并配置 Job
        Job job = Job.getInstance(conf);
        job.setJarByClass(Driver.class);
        job.setMapperClass(InvertedMapper.class);
        // Combiner 做本地合并以减少网络传输（对计数类问题安全有效）
        job.setCombinerClass(InvertedCombiner.class);
        // Reducer 使用 TableReducer 写入 HBase
        job.setReducerClass(InvertedReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 调整 reduce shuffle 内存占比以避免 OOM（示例参数，可根据集群调优）
        job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");

        // 输入路径从命令行读取，保持通用性
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        // 将 Reducer 初始化为写入 HBase 的 TableReducer，表名请根据实际修改
        TableMapReduceUtil.initTableReducerJob("InvertedIndexTable",InvertedReducer.class,job);
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);
    }
}