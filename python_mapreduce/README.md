# Python MapReduce（Hadoop Streaming） — 倒排索引示例（中文说明）

此目录包含一个使用 Hadoop Streaming 的小型倒排索引管道示例：

- `mapper.py`：将每个单词与所属文件关联，输出格式为 `word:filename\t1`。
- `combiner.py`：在 map 端本地聚合相同 `word:filename` 的计数，输出 `word\tfilename:count`，从而减少 shuffle 数据量。
- `reducer.py`：将 `filename:count` 聚合为 `word\tfile1:cnt1;file2:cnt2;...`，并合并同一文件的多次计数。
- `run_streaming.sh`：提交 Hadoop Streaming 作业的脚本，已提供参数化选项。
- `load_to_hbase.sh`：把 reducer 的输出加载到 HBase（使用 hbase shell 的 put 命令）。

## 目录结构

    python_mapreduce/
	├─ mapper.py
	├─ combiner.py
	├─ reducer.py
	├─ run_streaming.sh
	└─ load_to_hbase.sh

## 运行前准备

1. 确保 HDFS 已就绪，Hadoop 环境变量（如 `HADOOP_HOME`）已正确设置。
2. 把待处理的小文件上传到 HDFS，例如：`/input/sentences/files/`（每个文件为一段句子或文本）。
3. （可选）在所有节点上确认 Python3 可用且可执行 `#!/usr/bin/env python3` 脚本。

## 运行示例

1) 提交默认作业（使用脚本内默认路径）：

```bash
cd python_mapreduce
./run_streaming.sh
```

2) 指定输入与输出路径：

```bash
./run_streaming.sh -i /input/sentences/files -o /user/sun/my_output_dir -w /tmp/mywork
```

## 选项说明

- `-i`: HDFS 输入目录（脚本会对目录下的所有文件提交作业）。
- `-o`: HDFS 输出目录（若已存在会导致 Hadoop 作业失败，请事先删除或改名）。
- `-w`: 本地工作目录（脚本会在此创建临时文件）。
- `-s`: 指定 Hadoop Streaming jar 的完整路径（如果未设置 `HADOOP_HOME` 可以使用此选项）。

## 产出与下游

- Hadoop job 完成后，输出在你指定的 HDFS 输出目录下，通常是若干 `part-xxxxx` 文件。
- 使用 `hdfs dfs -cat <output>/part-* | head` 可以查看样例结果，格式为：

  `word\tfile1:cnt1;file2:cnt2;...`
- `load_to_hbase.sh` 脚本可以把这些行转换为 HBase 的 `put` 命令并导入表中（请先保证 HBase 可用，并修改脚本中的表名和列族）。

## 常见问题与排查

- 找不到 streaming jar：请检查 `HADOOP_HOME` 是否设置正确，或使用 `-s` 指定 jar 路径。
- 作业失败：检查 YARN/MapReduce 日志（ResourceManager / ApplicationMaster / Container 日志）。
- 输出目录已存在：Hadoop 会拒绝写入已存在的输出目录，先用 `hdfs dfs -rm -r <out>` 删除再重试。
- Python 脚本编码/异常：在本地先用 `python3 mapper.py < sample.txt` 测试输入输出是否正常。

## 调试建议

- 在本地运行 mapper/combiner/reducer 的模拟输入测试，以保证它们在集群上能按预期工作：

```bash
cat local_sample.txt | python3 mapper.py | sort | python3 combiner.py | sort | python3 reducer.py
```

- 当遇到 HBase 写入问题时，可先把 reducer 输出保存到本地并人工检查格式，再使用 `hbase shell` 执行 `put`。

## 安全与兼容性提示

- 这些脚本尽量做到容错：会跳过格式异常的输入行，避免单条错误导致任务失败。但请尽量保证输入格式干净。
- 如果需要对单词做归一化（如小写、去除标点、分词等），建议在 mapper 中加入明确的预处理逻辑，或在上传 HDFS 前先做离线清洗。

## 后续可选改进

- 把输出直接写入 HBase（使用 Python HBase 客户端而非 hbase shell），提高写入效率并避免 shell 注入风险。
- 为大规模文件读写增加批量提交和重试机制。
