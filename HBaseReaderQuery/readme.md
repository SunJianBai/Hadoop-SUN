伪分布查询

功能说明
- 输入表名和 RowKey，程序会在 HBase 中查询对应数据并打印到终端。
- 示例：查询倒排索引表 `InvertedIndexTable` 中 `apple` 的数据。

编译与打包
在项目根目录下执行：
```bash
mvn clean package -DskipTests
```
打包成功后，生成的 jar 在：
target/HBaseReaderQuery-1.0-SNAPSHOT.jar

运行
``` bash
java -cp target/HBaseReaderQuery-1.0-SNAPSHOT.jar:$(hbase classpath) \
     com.test.HBaseReaderQuery InvertedIndexTable apple
``` 
输出
```bash
RowKey = apple
Value  = file1.txt:3;file2.txt:5;
```


