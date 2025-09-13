Web UI for querying HBase via existing Java jar (HBaseReaderQuery)

可视化的搜索引擎，可以查找单词出现的位置和数量

快速说明

1. 在运行本应用前，请确保 `HBaseReaderQuery/target/HBaseReaderQuery-1.0-SNAPSHOT.jar` 已构建存在。
   如果没有，请在 `HBaseReaderQuery` 目录下构建（例如使用 `mvn package` 或 `mvn -DskipTests package`）。
2. 安装依赖并运行：

```bash
cd web_query
python3 -m pip install -r requirements.txt
python3 app.py
```

3. 打开浏览器访问 `http://localhost:5000/`，在搜索框输入单词并回车，页面将调用 Java jar 进行查询并展示结果。

注意

- 需要在执行环境中能运行 `hbase classpath` 命令（脚本会用它来组装 classpath）。
- 若在容器/远端环境运行，请确保 Java 和 HBase 客户端可用且网络能访问 HBase。
