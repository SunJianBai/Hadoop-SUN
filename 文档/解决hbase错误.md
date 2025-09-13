# HBase启动报错“Master is initializing”解决方案

---

## 环境说明

- 操作系统：ubuntu 24.04
- Hadoop：3.3.0
- HBase：2.4.18
- JDK：1.8.0_461
- Zookeeper：3.7.2

---

## 报错消息
Hbase 可以启动，但无法进行任何操作。
在 HBase shell 执行 `list`，`create` 等命令时出现如下错误：

```shell
hbase(main):001:0> list
TABLE

ERROR: org.apache.hadoop.hbase.PleaseHoldException: Master is initializing
at org.apache.hadoop.hbase.master.HMaster.checkInitialized(HMaster.java:2452)
...
```

提示 Master 正在初始化，无法正常操作。

---

## 问题原因分析

Hadoop 和 Zookeeper 的数据目录中存在残留数据（脏数据），导致 HBase 启动时初始化失败。需要清理 HDFS 的 `/hbase` 目录和 Zookeeper 的 `/hbase` 节点，并重新初始化相关服务。

---

## 详细解决步骤

### 1. 清理 Hadoop

**步骤：**

1. 关闭所有 HBase 服务：
    ```bash
    stop-hbase.sh
    ```

2. 关闭所有 Hadoop 服务：
    ```bash
    stop-all.sh
    ```

3. 确认所有相关进程已关闭：
    ```bash
    jps
    # 仅显示 Jps 进程即可
    ```

4. 启动 Hadoop 服务：
    ```bash
    start-all.sh
    ```

5. 查看 HDFS 根目录文件：
    ```bash
    hdfs dfs -ls /
    # 应包含 /hbase 目录
    ```

6. 删除 HDFS 中的 /hbase 目录：
    ```bash
    hdfs dfs -rm -r /hbase
    ```

---

### 2. 清理 Zookeeper

**步骤：**

1. 确保 Zookeeper 已启动。
    ```bash
    zkServer.sh start
    ```

2. 进入 Zookeeper 的 bin 目录：
    ```bash
    cd /opt/module/zookeeper/bin
    ```

3. 启动 Zookeeper 客户端：
    ```bash
    zkCli.sh
    ```
   如果是多个节点集群，请使用如下命令：
    ```bash
    zkCli.sh -server master:2181,worker2:2181,worker1:2181
    ```

4. 查看根目录内容：
    ```shell
    [zk: localhost:2181(CONNECTED) 0] ls /
    # 若有 hbase 节点，继续下一步
    ```

5. 删除 Zookeeper 中的 /hbase 节点：
    ```shell
    [zk: localhost:2181(CONNECTED) 1] deleteall /hbase
    # 若提示 Node does not exist: /hbase，说明已无残留
    ```

---

### 3. 重启 HBase

**步骤：**

1. 启动 HBase 服务：
    ```bash
    cd /usr/local/hbase
    bin/start-hbase.sh
    ```

2. 检查 HBase 状态：
    ```bash
    jps
    # 应有 HMaster、HRegionServer 等进程
    ```

3. 进入 HBase shell 测试：
    ```bash
    bin/hbase shell
    list
    # 应能正常显示表信息,也可以正常建表
    ```

---

## 注意事项

- 清理数据前请备份重要数据，避免误删导致数据丢失。
- 删除 Zookeeper 节点时需确保 Zookeeper 服务已启动。
- 若多节点集群，需在所有节点执行相关清理操作。

---
