# 文件说明

- `scripts` 目录下的是脚本文件
  - `scripts\bak` 目录下是全部的配置文件，有完全分布的和伪分布的，以及用于覆盖配置文件的脚本
  - `scripts/sent.sh` 用于把本地的配置文件发送到其他节点机器上，并覆盖对方的文件。
  - `scripts/start.sh , stop.sh`  用于启动和停止本地的`hadoop`服务
  - `scripts/wordcut.py` 用于将大数据集分割成小块
  - `scripts/upload_hdfs.py` 把本地分割好的数据集上传到`HDFS`里
- `java_src` 目录下是 `MapReduce` 操作的核心代码
  -  具体说明在 `java_src/README.md`  

- `HBaseReaderQuery` 目录下是查询数据，验证环节的核心代码
  -  具体说明在 `HBaseReaderQuery/README.md`  

- `web_query` 目录下是web UI的核心代码
  -  具体说明在 `web_query/README.md`  



# 运行流程

## 处理原始数据

初始数据集文本在 `data/sentences/sentences.txt`
运行 `scripts/wordcut.py`脚本，将原始数据集进行分割，生成多个文件放在 `data/sentences/files`目录下

## 创建目录

运行 `scripts/mkdir_hdfs.py`脚本，在 `HDFS`创建目录结构

## 上传数据

运行 `scripts/upload_hdfs.py`脚本，将 `data/sentences/files`目录下的文件上传到 `HDFS`的 `/input/sentences/files`目录下
如果运行 `scripts/upload_hdfs_small.py`脚本，则只上传 `data/sentences/files`目录下的前20个文件

## 创建HBase表

Hbase需要的表名为 `InvertedIndexTable`，一个列族名为 `info`
进入hbase shell

```bash
hbase shell
```

创建表

```bash
create 'InvertedIndexTable', 'info'
```

清除表中残留的数据：

```bash
truncate "InvertedIndexTable"
```

## 运行MapReduce

### 编译jar包

进入java_src/InvertedMapReduce目录下

```bash
cd java_src/InvertedMapReduce
```

编译：

```bash
./gradlew build
```

### 使用 jar 包

编译完成后，jar 包一般在：

> build/libs/
> 例如：build/libs/InvertedMapReduce.jar

具体名字取决于构建脚本 `build.gradle`中 `version`字段

### 运行程序

其中jar包的名字根据实际运行情况来填写

```bash
hadoop jar build/libs/InvertedMapReduce-1.0-SNAPSHOT.jar /input/sentences/files
```

跑完MapReduce后会把处理结果存入Hbase数据库中



### 查询结果

构建编译 `HBaseReaderQuery` 目录下的代码，获得jar包 `HBaseReaderQuery-1.0-SNAPSHOT.jar` 

运行命令，最后面的单词是要查询的单词，根据实际需求更改

```bash
java -cp HBaseReaderQuery/target/HBaseReaderQuery-1.0-SNAPSHOT.jar:$(hbase classpath) \
     com.test.HBaseReaderQuery InvertedIndexTable apple
```

输出结果，Value 的内容是这个单词出现的文件位置和次数

```bash
RowKey = apple
Value  = file1.txt:3;file2.txt:5;
```



### web UI

运行`web_query/app.py` , 这是基于Flask框架的web ui，调用了前面生成的jar包，可以可视化查询单词。

运行后在 http://127.0.0.1:5000  查看![image-20250913204018306](https://raw.githubusercontent.com/SunJianBai/pictures/main/img/202509132040527.png)





