#!/usr/bin/env bash
set -euo pipefail

# 可配置项（默认值）
HDFS_INPUT_DIR="/input/sentences/files"
HDFS_OUTPUT_DIR="/user/$USER/python_mapreduce_output_$(date +%s)"
WORK_DIR="/tmp/python_mapreduce_run"

usage(){
  cat <<EOF
Usage: $0 [-i hdfs_input_dir] [-o hdfs_output_dir] [-w work_dir] [-s streaming_jar]

默认 HDFS 输入目录: $HDFS_INPUT_DIR
默认输出目录: $HDFS_OUTPUT_DIR
默认工作目录: $WORK_DIR
EOF
}

while getopts ":i:o:w:s:h" opt; do
  case $opt in
    i) HDFS_INPUT_DIR="$OPTARG" ;;
    o) HDFS_OUTPUT_DIR="$OPTARG" ;;
    w) WORK_DIR="$OPTARG" ;;
    s) STREAM_JAR="$OPTARG" ;;
    h) usage; exit 0 ;;
    \?) echo "Invalid option: -$OPTARG" >&2; usage; exit 1 ;;
  esac
done

mkdir -p "$WORK_DIR"

if [ -z "${STREAM_JAR:-}" ]; then
  if [ -z "${HADOOP_HOME:-}" ]; then
    echo "错误：未设置 HADOOP_HOME，也未通过 -s 指定 streaming jar。请设置 HADOOP_HOME 或使用 -s 指定 jar 路径。" >&2
    exit 2
  fi
  STREAM_JAR="$HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar"
fi

echo "提交 Hadoop Streaming 作业"
echo "  输入: $HDFS_INPUT_DIR"
echo "  输出: $HDFS_OUTPUT_DIR"
echo "  工作目录: $WORK_DIR"

hadoop jar $STREAM_JAR \
  -files mapper.py,combiner.py,reducer.py \
  -mapper "./mapper.py" \
  -combiner "./combiner.py" \
  -reducer "./reducer.py" \
  -input "$HDFS_INPUT_DIR/*" \
  -output "$HDFS_OUTPUT_DIR"

echo "Job finished. Output in HDFS under $HDFS_OUTPUT_DIR"
