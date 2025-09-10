#!/bin/bash
# run_streaming_examples.sh
# 目的：提供一键示例，展示如何使用 Hadoop Streaming 运行简单的 MapReduce 作业。
# 该脚本包含三个示例：
#  1) 统计每部电影被评分次数（mapper_count_movie.py / reducer_count_movie.py）
#  2) 计算每部电影的平均评分（mapper_avg.py / reducer_avg.py）
#  3) 把 movie_id 替换为电影标题（reducer_join_title.py + 本地 u.item 文件）
#
# 注意：该脚本默认以 YARN（集群）模式提交作业；若你的 YARN 环境有问题（例如
# AM 启动失败），可以用 local 模式回退（see below）。

set -euo pipefail

# 如果你没有在环境变量里设置 HADOOP_HOME，可以在这里修改为你的路径
: ${HADOOP_HOME:=/usr/local/hadoop}
STREAMING_JAR="${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-3.3.0.jar"

# 检查 streaming jar 是否存在，如果不存在脚本会退出并提示你修改 HADOOP_HOME
if [ ! -f "$STREAMING_JAR" ]; then
  echo "找不到 hadoop-streaming jar: $STREAMING_JAR" >&2
  echo "请确认 HADOOP_HOME 指向你的 Hadoop 安装目录，或手动把 streaming jar 路径修改到本脚本。" >&2
  exit 1
fi

# HDFS 上的数据目录（脚本假设你已经用 upload_ml100k_to_hdfs.sh 上传过数据）
HDFS_BASE="/user/$USER/input/ml-100k"

### 1) 统计每部电影被评分次数
echo "1) 统计每部电影被评分次数 (movie_counts)"
OUT1="/user/$USER/output/movie_counts"
# 删除老的输出目录（Hadoop 不允许输出目录已存在），-skipTrash 可以直接删除而不进回收
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash "$OUT1" || true

# 提交 Hadoop Streaming 作业：指定 mapper/reducer 并把本地脚本通过 -file 分发给每个 task
"$HADOOP_HOME/bin/hadoop" jar "$STREAMING_JAR" \
  -input "${HDFS_BASE}/u.data" \
  -output "$OUT1" \
  -mapper scripts/ml-100/mapper_count_movie.py \
  -reducer scripts/ml-100/reducer_count_movie.py \
  -file scripts/ml-100/mapper_count_movie.py \
  -file scripts/ml-100/reducer_count_movie.py

echo "结果示例 (top 20):"
# 把输出按计数排序并展示 top20，注意输出在 HDFS 上，需要用 hdfs dfs -cat 查看
$HADOOP_HOME/bin/hdfs dfs -cat "$OUT1/part-*" | sort -k2 -nr | head -n 20


### 2) 计算每部电影平均评分
echo
echo "2) 计算每部电影平均评分 (movie_avg)"
OUT2="/user/$USER/output/movie_avg"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash "$OUT2" || true

"$HADOOP_HOME/bin/hadoop" jar "$STREAMING_JAR" \
  -input "${HDFS_BASE}/u.data" \
  -output "$OUT2" \
  -mapper scripts/ml-100/mapper_avg.py \
  -reducer scripts/ml-100/reducer_avg.py \
  -file scripts/ml-100/mapper_avg.py \
  -file scripts/ml-100/reducer_avg.py

echo "平均评分示例 (按 avg 倒序显示 top 20):"
$HADOOP_HOME/bin/hdfs dfs -cat "$OUT2/part-*" | sort -k2 -nr | head -n 20


### 3) 用电影名替换 movie_id（join 操作）
echo
echo "3) 用电影名替换 movie_id（join）"
OUT3="/user/$USER/output/movie_avg_named"
$HADOOP_HOME/bin/hdfs dfs -rm -r -skipTrash "$OUT3" || true

# -files/-file 参数会把本地文件分发到每个 reducer 的工作目录；这里我们把 u.item 一并传送
"$HADOOP_HOME/bin/hadoop" jar "$STREAMING_JAR" \
  -input "$OUT2" \
  -output "$OUT3" \
  -mapper /bin/cat \
  -reducer scripts/ml-100/reducer_join_title.py \
  -file scripts/ml-100/reducer_join_title.py \
  -file data/ml-100k/ml-100k/u.item

echo "命名结果示例："
$HADOOP_HOME/bin/hdfs dfs -cat "$OUT3/part-*" | head -n 30

echo "运行完成。"