#!/usr/bin/env bash
# 备份 sent.sh 中列出的改动文件到本地完全分布目录（保留目录结构与权限）
# 用法: ./备份.sh
set -euo pipefail

# 本脚本放在 scripts/bak/ 下，目标备份目录为 scripts/bak/完全分布
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEST_ROOT="$SCRIPT_DIR/完全分布"

# 与 sent.sh 中相同的文件列表（来源）
FILES_SRC=(
  "/etc/hosts"
  "/usr/local/hadoop/etc/hadoop/core-site.xml"
  "/usr/local/hadoop/etc/hadoop/hdfs-site.xml"
  "/usr/local/hadoop/etc/hadoop/yarn-site.xml"
  "/usr/local/hadoop/etc/hadoop/mapred-site.xml"
  "/usr/local/hadoop/etc/hadoop/workers"
  "/usr/local/zookeeper/conf/zoo.cfg"
  "/usr/local/hbase/conf/hbase-site.xml"
  "/usr/local/hbase/conf/hbase-env.sh"
  "/usr/local/hbase/conf/regionservers"
)

echo "备份目标目录： $DEST_ROOT"
mkdir -p "$DEST_ROOT"

for src in "${FILES_SRC[@]}"; do
  if [ -e "$src" ]; then
    dest="$DEST_ROOT$src"
    dest_dir="$(dirname "$dest")"
    echo "备份: $src -> $dest"
    mkdir -p "$dest_dir"
    # 先尝试普通复制，若无权限则用 sudo 复制
    if cp --preserve=mode,timestamps "$src" "$dest" 2>/dev/null; then
      :
    else
      echo "  无权限，尝试使用 sudo 复制..."
      sudo cp --preserve=mode,timestamps "$src" "$dest"
    fi
    # 尝试保留原文件拥有者信息（需要 sudo）
    if chown --reference="$src" "$dest" 2>/dev/null; then
      :
    else
      sudo chown --reference="$src" "$dest" 2>/dev/null || true
    fi
  else
    echo "警告: 源文件不存在，跳过: $src"
  fi
done

echo "全部文件已备份到 $DEST_ROOT"