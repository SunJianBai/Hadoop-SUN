

------

#  MapReduce 的倒排索引

## 实验背景

MapReduce 是由 **Hadoop** 提供的开源软件框架，能够在由上千台商用机器组成的大规模集群上，以一种**可靠**、**高容错**的方式并行处理 **TB 级别的海量数据集**。

在本次实验中，我们使用MapReduce框架实现了词频统计及倒排索引的Java程序，能够在hadoop上分布式运行，并将处理结果导入进hbase当中。

- **开发环境**：JDK 1.8.0_461
- **构建工具**：Gradle（自动导入 Hadoop/MapReduce 相关依赖）

------

## build.gradle 配置

```json
plugins {
    id 'java'
    id 'application'
}

group = 'com.test'
version = '1.2-Localhost'
sourceCompatibility = '1.8'
targetCompatibility = '1.8'

repositories {
    mavenCentral()
}

dependencies {
    implementation 'org.apache.hadoop:hadoop-common:3.3.5'
    implementation 'org.apache.hadoop:hadoop-client:3.3.5'
    implementation 'org.apache.hbase:hbase-common:2.5.4'
    implementation 'org.apache.hbase:hbase-client:2.5.4'
    implementation 'org.apache.hbase:hbase-mapreduce:2.5.4'
}

application {
    mainClass = 'com.test.Driver'
}

jar {
    manifest {
        attributes 'Main-Class': 'com.test.Driver'
    }
    from {
        configurations.runtimeClasspath.collect { it.isDirectory() ? it : zipTree(it) }
    }
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
}
```

**设计说明**：
 该 `build.gradle` 文件配置了 Java 项目所需的依赖和构建规则。

- 使用 `mavenCentral()` 作为仓库，能够自动下载 Hadoop 与 HBase 的依赖库。
- 依赖部分引入了 Hadoop 核心组件、客户端接口，以及 HBase 的常用组件和 MapReduce 集成包。
- `application` 与 `jar` 部分确保生成的 JAR 包包含正确的主类信息 (`Main-Class`)，并打包运行时依赖，方便在分布式环境中直接运行。

------

## Map 阶段

### InvertedMapper.java

```java
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
```

**设计说明**：

- **输入数据**：
   每一行包含一个编号和若干单词，格式如：`1 word1 word2 word3`。
  - `LongWritable key`：表示行号或文件偏移量，在本实现中未被使用。
  - `Text value`：表示文件中的一行文本数据。
- **处理逻辑**：
  1. 将每一行按空格分割，忽略首列编号，仅保留单词序列。
  2. 通过 `FileSplit` 获取输入文件的文件名，以便在倒排索引中区分不同来源文件。
  3. 遍历每个单词，构造键值对 `<word:filename, 1>`。
- **输出结果**：
  - key：`word:filename`，用于标记单词在哪个文件中出现。
  - value：`1`，表示出现了一次。

**算法核心**

**`map()` 方法**：

- **输入**：
  - `key`：行号（LongWritable类型），在这段代码中没有使用。
  - `value`：表示一行输入文本（Text类型）。
  - `context`：提供了与Hadoop框架通信的上下文，允许Mapper将结果输出。
- **处理过程**：
  - **拆分行文本**：使用 `value.toString().split(" ")` 将输入的文本行按空格拆分为一个单词数组。拆分后的第一个元素（句子编号）被忽略，后续的元素是句子中的单词。
  - **获取文件名**：通过 `FileSplit` 获取当前处理的输入文件名，这个文件名会与每个单词组合在一起，用作键的一部分。
  - **构建键值对**：通过遍历句子中的每个单词，将单词与文件名组合（格式为 `word:filename`），然后将该组合作为键，"1" 作为值（代表该单词在该文件中出现过一次），并输出到 `context` 中。

------

## Combiner 阶段

### InvertedCombiner.java

```java
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

```

**设计说明**：

- **输入数据**：
   Mapper 输出的中间结果 `<word:filename, 1>`。同一个文件的相同单词会出现多次。
- **处理逻辑**：
  1. 遍历所有值 `"1"`，累加得到该单词在文件中的总出现次数。
  2. 将原始 key（`word:filename`）拆分为单词和文件名。
  3. 输出新的键值对 `<word, filename:count>`。
- **输出结果**：
  - key：单词（如 `word1`）
  - value：文件名和出现次数（如 `file1:3`）
- **作用**：
   Combiner 是 **局部 Reducer**，它在 Map 阶段本地执行，能够有效减少需要传输到真正 Reducer 的数据量，提升整体性能。

------

## Reduce 阶段

### InvertedReducer.java

```java
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

```

**设计说明**：

- **输入数据**：
   Combiner 的输出 `<word, filename:count>`。同一个单词可能对应多个不同文件。
- **处理逻辑**：
  1. 将所有 `filename:count` 拼接为以分号分隔的字符串，例如：`file1:3;file2:5;file3:1;`。
  2. 使用单词作为行键（rowkey），构建 HBase 的 `Put` 对象。
  3. 将结果存储到 HBase 表的 `info:index` 列中。
- **输出结果**：
  - HBase 表中一行数据：
    - rowkey = 单词
    - 列族 = `info`
    - 列名 = `index`
    - 值 = `filename:count;filename2:count2;...`
- **作用**：
   最终得到倒排索引表，实现从单词到文件集合的映射。借助 HBase，可以直接进行分布式查询，满足大规模文本检索需求。

------

## Driver 程序

### Driver.java

```java
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
```

**设计说明**：

- **核心职责**：作为整个 MapReduce 作业的入口，配置并启动任务。
- **配置内容**：
  - `fs.defaultFS`：HDFS 的 NameNode 地址。
  - `yarn.resourcemanager.hostname`：YARN 的资源调度管理节点。
  - `hbase.zookeeper.quorum`：HBase 使用的 ZooKeeper 集群地址。
- **作业设置**：
  - Mapper → `InvertedMapper` ：指定作业的 Mapper 类，负责将输入数据拆分并生成键值对。
  - Combiner → `InvertedCombiner` ：指定作业的 Combiner 类，用于在 Mapper 输出后，Reducer 之前对数据进行本地聚合。
  - Reducer → `InvertedReducer` ：指定 Reducer 类，用于最终的聚合和结果输出。
  - 输入路径通过命令行参数传递，保持通用性。
- **输出结果**：
   使用 `TableMapReduceUtil` 直接将 Reducer 的结果写入 HBase 表 `InvertedIndexTable`，作业的输出不是写入 HDFS 文件系统，而是直接插入到 HBase 表中。通过这种方式，避免额外的导入步骤，提升性能和易用性。

------

