# 



# 运行流程
## 处理原始数据
初始数据集文本在`data/sentences/sentences.txt`
运行`scripts/wordcut.py`脚本，将原始数据集进行分割，生成多个文件放在`data/sentences/files`目录下


## 创建目录
运行`scripts/mkdir_hdfs.py`脚本，在`HDFS`创建目录结构

## 上传数据
运行`scripts/upload_hdfs.py`脚本，将`data/sentences/files`目录下的文件上传到`HDFS`的`/input/sentences/files`目录下
如果运行`scripts/upload_hdfs_small.py`脚本，则只上传`data/sentences/files`目录下的前20个文件

## 创建HBase表

Hbase需要的表名为`InvertedIndexTable`，一个列族名为`info`
进入hbase shell
```bash
hbase shell
```
创建表
```bash
create 'InvertedIndexTable', 'info'
```


## 运行程序
### 编译jar包
进入java_src/InvertedMapReduce目录下
```bash
cd java_src/InvertedMapReduce
```

编译：
```bash
./gradlew build
```

### 查找 jar 包
编译完成后，jar 包一般在：
> build/libs/
> 例如：build/libs/InvertedMapReduce.jar

具体名字取决于构建脚本`build.gradle`中`version`字段

### 运行程序
其中jar包的名字根据实际运行情况来填写
```bash
hadoop jar build/libs/InvertedMapReduce-1.0-SNAPSHOT.jar /input/sentences/files
```



