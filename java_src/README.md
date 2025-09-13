>   MapReduce是由hadoop提供的一个开源软件框架，基于该框架能够容易地编写应用程序运行在由上千个商用机器组成的大集群上，并以一种可靠的，具有容错能力的方式并行地处理上TB级别的海量数据集。

  在本次实验中，我们使用MapReduce框架实现了词频统计及倒排索引的Java程序，能够在hadoop上分布式运行，并将处理结果导入进hbase当中。
  Java项目使用jdk1.8.0_461。采用gradle构建自动导入hadoop上MapReduce框架的相关依赖。

## build.gradle配置文件

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

## Map阶段算法说明

- **InvertedMapper.java代码**

  ```java
  package com.test;

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
- **类定义**

    InvertedMapper类继承自 `Mapper<LongWritable, Text, Text, Text>`，其中 `Mapper` 是Hadoop中的核心类，用于将输入数据映射为键值对。它实现了 `map()` 方法，处理输入数据并生成中间结果，供后续的Reducer处理。
- **数据结构与定义**

  1. **输入数据格式**：

  - `LongWritable key`：文件中当前处理行的偏移量，作为行号。
  - `Text value`：代表文件中的一行数据，假设每行数据包含一个句子编号及其对应的句子文本。

  1. **输出键值对**：

  - **键 (`Text`)**：格式为 `word:filename`，表示某个单词出现在某个文件中。
  - **值 (`Text`)**：固定为 `"1"`，表示每次遇到该单词在该文件中出现一次。
- **算法核心**

  **`map()` 方法**：

  - **输入**：
    - `key`：行号（LongWritable类型），在这段代码中没有使用。
    - `value`：表示一行输入文本（Text类型）。
    - `context`：提供了与Hadoop框架通信的上下文，允许Mapper将结果输出。
  - **处理过程**：
    - **拆分行文本**：使用 `value.toString().split(" ")` 将输入的文本行按空格拆分为一个单词数组。拆分后的第一个元素（句子编号）被忽略，后续的元素是句子中的单词。
    - **获取文件名**：通过 `FileSplit` 获取当前处理的输入文件名，这个文件名会与每个单词组合在一起，用作键的一部分。
    - **构建键值对**：通过遍历句子中的每个单词，将单词与文件名组合（格式为 `word:filename`），然后将该组合作为键，"1" 作为值（代表该单词在该文件中出现过一次），并输出到 `context` 中。

## Combine阶段算法说明

- **InvertedCombiner.java代码**

  ```java
  package com.test;

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
- **类定义**

    `InvertedCombiner`类继承自 `Reducer<Text, Text, Text, Text>`：它实现了 `reduce()` 方法，将Mapper生成的中间结果进行局部聚合。输入键值对为 `<Text, Text>`，输出的键值对也是 `<Text, Text>`。
- **Combiner在MapReduce中的作用**

  - **Mapper阶段的输出**：在Mapper阶段，每行文本被处理后，生成的键值对是 `<word:filename, 1>`，表示某个单词在某个文件中出现了一次。对于同一个文件中的同一个单词，可能会产生多个键值对，如：

    ```
    <word1:file1, 1>
    <word1:file1, 1>
    <word2:file1, 1>
    ```
  - **Combiner阶段的优化**：Combiner相当于一个局部的Reducer，它的作用是对Mapper输出的结果进行局部合并，减少数据传输量。在 `InvertedCombiner` 中，它负责计算出同一文件中某个单词的总出现次数，并将结果重新格式化为 `<单词, 文件名:次数>` 的形式。例如：

    ```
    Mapper 输出: <word1:file1, 1>, <word1:file1, 1>
    Combiner 输出: <word1, file1:2>
    ```

    这样，在Reducer阶段传输的数据量大大减少，从而一定程度提高了性能。
- **算法实现**

  `reduce()` 方法：

  - **输入**：

    - `key`：一个文本类型的键，表示Mapper输出的键，格式为 `word:filename`。
    - `values`：一个 `Iterable<Text>` 集合，表示Mapper输出的所有值。每个值是 "1"，表示单词在该文件中出现一次。
  - **处理过程**：

    - **计算单词出现的总次数**：遍历 `values` 集合，累加每个值（这里每个值都是 "1"），得到单词在某个文件中的总出现次数。
    - **拆分键，调整输出格式**：通过 `key.toString().indexOf(":")` 找到 `word:filename` 字符串中 `:` 的位置，方便后续进行拆分。使用 `substring` 方法将 `key` 拆分为 `word` 和 `filename`。
    - **重设键值对**：将 `key` 设置为单词（仅保留单词部分，不再包含文件名）。将 `valueInfo` 设置为文件名和单词出现的总次数，格式为 `filename:count`。
  - **输出**：

      输出的键为单词，值为 `filename:count`，即 `<单词, 文件名:次数>` 的形式。

## Reduce阶段算法说明

- **InvertedIndexReducer.java代码**

  ```java
  package com.test;

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
- **类定义**

    `InvertedReducer`类继承自 `TableReducer<Text, Text, ImmutableBytesWritable>`。用于将输入的 key 和 values 进行处理，并将结果输出到 HBase 表中。
- **算法实现**

  `reduce()` 方法：

  - **输入**：
    - `key`：一个文本类型的键，表示Combiner输出的键，格式为 `word`。
    - `values`：一个 `Iterable<Text>` 集合，表示Combiner输出的所有值。每个值是 `filename:count`，表示单词在该文件中的总出现次数。
  - **处理过程**：
    - **字符串拼接**： 代码通过遍历 `Iterable<Text> values`，逐一获取每个 `Text` 值，并将它们拼接到 `fileList` 字符串中。每个值之间用 `;` 作为分隔符。
    - **构造 HBase Put 对象**： `Put` 是 HBase 的数据操作对象，用于将数据存储到 HBase 表中。代码通过将 `key` 转换为字节数组来创建一个 `Put` 对象。之后，通过 `put.addColumn()` 方法，向名为 `info` 的列族和 `index` 列中添加数据，值是拼接后的 `fileList` 字符串。
    - **写入结果到上下文**： `context.write(null, put)` 表示将构造好的 `Put` 对象写入到 HBase 表中。在这个例子中，`key` 是 null，这表示输出的行键已经在 `Put` 对象中定义。

## Driver阶段主函数

- **InvertedIndexDriver.java代码**

  ```java
  package com.test;

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
- **类定义**

  `Driver`作为Hadoop MapReduce的驱动程序，负责配置和启动作业。其核心任务是使用 `InvertedMapper`、`InvertedCombiner` 和 `InvertedReducer` 来处理 HDFS 中的数据，并将结果写入 HBase 表中。
- **算法实现**

  1. **异常处理**

     `main`方法抛出了一些异常，常见于 Hadoop 程序，如 `ClassNotFoundException`、`IOException` 和 `InterruptedException`，用于处理分布式作业的异常情况。
  2. **Configuration 配置对象**

     `Configuration conf = new Configuration();`：创建一个 Hadoop 配置对象 `conf`，用于存储作业的配置信息。Hadoop 集群的连接信息如下：

     - `conf.set("fs.defaultFS","hdfs://hadoop:9000");`：设置 HDFS 的主 NameNode 的地址，即文件系统的默认根路径。
     - `conf.set("yarn.resourcemanager.hostname","hadoop");`：配置 Yarn 的 ResourceManager 地址，负责作业的资源调度。
     - `conf.set("hbase.zookeeper.quorum","hadoop");`：配置 Zookeeper 集群的地址，Zookeeper 用于管理 HBase 的 HMaster。
  3. **作业初始化**

     - `Job job = Job.getInstance(conf);`：创建一个新的 `Job` 实例，用于配置和管理 MapReduce 作业。
     - `job.setJarByClass(Driver.class);`：设置包含主类的 JAR 文件，这个 JAR 包含作业的执行代码，并且可以在分布式节点上运行。
  4. **设置 Mapper、Combiner 和 Reducer 类**

     - `job.setMapperClass(InvertedMapper.class);`：指定作业的 Mapper 类，负责将输入数据拆分并生成键值对。
     - `job.setCombinerClass(InvertedCombiner.class);`：指定作业的 Combiner 类，用于在 Mapper 输出后，Reducer 之前对数据进行本地聚合。
     - `job.setReducerClass(InvertedReducer.class);`：指定 Reducer 类，用于最终的聚合和结果输出。
  5. **设置 Map 输出的键值类型**

     - `job.setMapOutputKeyClass(Text.class);`：Mapper 输出的键的类型为 `Text`，即 Hadoop 的文本类型。
     - `job.setMapOutputValueClass(Text.class);`：Mapper 输出的值的类型也是 `Text`，即文本数据。
  6. **其他配置**

     - `job.getConfiguration().setStrings("mapreduce.reduce.shuffle.memory.limit.percent", "0.15");`：设置 Reduce 任务的内存限制，用于限制 MapReduce 过程中的内存使用。
  7. **输入路径配置**

     - `FileInputFormat.setInputPaths(job, new Path(args[0]));`：通过 `FileInputFormat` 设置作业的输入路径，该路径来自命令行参数 `args[0]`。
  8. **与 HBase 的集成**

     ```
     TableMapReduceUtil.initTableReducerJob("InvertedIndexTable", InvertedReducer.class, job);
     ```
       这行代码使用 `TableMapReduceUtil` 工具类将结果输出到 HBase 表中，指定表名为 `"InvertedIndexTable"`。第二个参数 `InvertedReducer.class` 是 Reducer 类，用于将结果写入 HBase。通过这种方式，作业的输出不是写入 HDFS 文件系统，而是直接插入到 HBase 表中。
