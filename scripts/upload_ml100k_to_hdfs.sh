#!/bin/bash
# 脚本用途：把 ml-100k 的关键文件 (u.data, u.item, u.user) 上传到 HDFS，并做基本校验与设置

set -euo pipefail

# 本地数据目录（你给出的路径，使用绝对路径以避免在不同工作目录下出错）
LOCAL_DIR="data/ml-100k/ml-100k"

# 目标 HDFS 目录（按 user 隔离，推荐放 /user/$USER/input）
HDFS_BASE="/user/$USER/input"
HDFS_TARGET="${HDFS_BASE}/ml-100k"

# 尝试找到 hdfs 命令：优先使用 PATH 中的 hdfs，否则使用 HADOOP_HOME 或 /usr/local/hadoop
if command -v hdfs >/dev/null 2>&1; then
  HDFS_CMD="$(command -v hdfs)"
elif [ -n "${HADOOP_HOME:-}" ] && [ -x "${HADOOP_HOME}/bin/hdfs" ]; then
  HDFS_CMD="${HADOOP_HOME}/bin/hdfs"
else
  HDFS_CMD="/usr/local/hadoop/bin/hdfs"
fi

echo "使用 HDFS 命令: $HDFS_CMD"

# 1) 检查本地数据目录是否存在；若不存在立即退出（避免上传错误位置）
if [ ! -d "$LOCAL_DIR" ]; then
  echo "本地数据目录不存在: $LOCAL_DIR"
  exit 1
fi

# 2) 检查 HDFS 是否可用（尝试列根目录），失败说明 NameNode 可能未启动
echo "检查 HDFS 可用性..."
if ! $HDFS_CMD dfs -ls / >/dev/null 2>&1; then
  echo "无法访问 HDFS：请确认 NameNode/DataNode 已启动（jps 查看 NameNode）"
  exit 1
fi

# 3) 在 HDFS 上创建目标目录（-p：递归创建，若已存在不会报错）
# 作用：为数据上传建立规范路径，便于后续 MapReduce/YARN 默认路径使用
echo "创建 HDFS 目标目录：$HDFS_TARGET"
$HDFS_CMD dfs -mkdir -p "$HDFS_TARGET"

# 4) 上传文件前的确认（仅列出将要上传的本地文件，避免误操作）
echo "将上传以下文件（本地）："
ls -l "$LOCAL_DIR"/u.data "$LOCAL_DIR"/u.item "$LOCAL_DIR"/u.user || true

# 5) 如果目标文件已存在，先备份或删除（这里选择覆盖前删除，避免 put 失败）
# 说明：hdfs dfs -put 默认不覆盖同名文件；在开发环境常先删除再上传
for f in u.data u.item u.user; do
  if $HDFS_CMD dfs -test -e "${HDFS_TARGET}/${f}"; then
    echo "HDFS 上已存在 ${HDFS_TARGET}/${f}，将先删除后上传"
    $HDFS_CMD dfs -rm -skipTrash "${HDFS_TARGET}/${f}"
  fi
done

# 6) 上传文件到 HDFS（-put 等价 copyFromLocal）
# 说明：按文件上传，便于后续单独处理与权限设置
echo "开始上传文件到 HDFS..."
$HDFS_CMD dfs -put "$LOCAL_DIR"/u.data "$HDFS_TARGET"/u.data
$HDFS_CMD dfs -put "$LOCAL_DIR"/u.item "$HDFS_TARGET"/u.item
$HDFS_CMD dfs -put "$LOCAL_DIR"/u.user "$HDFS_TARGET"/u.user

# 7) 单机伪分布环境建议把副本数设为 1，避免占用多份磁盘空间
# 说明：在单机环境设置 dfs.replication=1 或显式 setrep -R 1 可节省空间
echo "设置 HDFS 副本数为 1（递归）"
$HDFS_CMD dfs -setrep -R -w 1 "$HDFS_TARGET"

# 8) 列出 HDFS 目录及文件大小，做上传校验
# 说明：确认文件已经到位，并查看占用大小
echo "上传后的 HDFS 列表："
$HDFS_CMD dfs -ls -h "$HDFS_TARGET"

# 9) 快速查看每个文件的前几行，确保内容格式正确（u.data 用 \t 分隔）
echo "查看 u.data 前 10 行（用于确认字段格式 user_id item_id rating timestamp）:"
# 临时关闭 pipefail，防止 hdfs dfs -cat 在 head 提前退出时导致脚本因 SIGPIPE 停止
set +o pipefail
$HDFS_CMD dfs -cat "${HDFS_TARGET}/u.data" | head -n 10 || true

echo "查看 u.item 前 5 行（movie metadata，| 分隔）:"
$HDFS_CMD dfs -cat "${HDFS_TARGET}/u.item" | head -n 5 || true

echo "查看 u.user 前 5 行（user metadata，| 分隔）:"
$HDFS_CMD dfs -cat "${HDFS_TARGET}/u.user" | head -n 5 || true
# 恢复 pipefail 行为（保留 set -e 的安全性）
set -o pipefail

# 10) 总结输出：HDFS 上该目录的占用情况
echo "HDFS 目录占用情况："
$HDFS_CMD dfs -du -h "$HDFS_TARGET"

echo "上传完成。可在 NameNode UI (http://localhost:9870) -> Browse the file system 检查文件。"