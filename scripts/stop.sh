#!/bin/bash
# 停止 Hadoop + ZooKeeper + HBase

echo "=== 停止 HBase ==="
pkill -f HRegionServer
pkill -f HMaster
# stop-hbase.sh
# sleep 5   # 给 HBase 一点时间释放 ZooKeeper 连接

echo "=== 停止 ZooKeeper ==="
zkServer.sh stop


echo "=== 停止 YARN ==="
stop-yarn.sh

echo "=== 停止 HDFS ==="
stop-dfs.sh

echo "=== 确认所有进程已停止 ==="
jps
