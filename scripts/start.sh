#!/bin/bash
# 启动 Hadoop + ZooKeeper + HBase

echo "=== 启动 HDFS (NameNode, DataNode, SecondaryNameNode) ==="
start-dfs.sh

echo "=== 启动 YARN (ResourceManager, NodeManager) ==="
start-yarn.sh

echo "=== 启动 ZooKeeper ==="
zkServer.sh start

echo "=== 启动 HBase ==="
start-hbase.sh

echo "=== 查看当前运行的 Java 进程 ==="
jps
