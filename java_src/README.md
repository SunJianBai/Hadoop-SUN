
```bash
cd java_src/InvertedMapReduce
```


jar包输出的表名为InvertedIndexTable

```bash
cd /home/sun/codes/大数据系统开发小学期/Hadoop-SUN/java_src/InvertedMapReduce

```

编译：
```bash
./gradlew build
```

查找 jar 包
编译完成后，jar 包一般在：

> build/libs/
> 例如：build/libs/InvertedMapReduce.jar









```bash
hadoop jar build/libs/InvertedMapReduce-1.0-SNAPSHOT.jar /input/sentences/files
```







