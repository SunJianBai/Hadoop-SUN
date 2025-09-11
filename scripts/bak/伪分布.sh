#!/usr/bin/env bash
# 将 scripts/bak/完全分布 中的备份文件恢复到对应系统位置
# 恢复前会提示并对目标已有文件做 .bak.TIMESTAMP 备份
# 用法: ./完全分布.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC_ROOT="$SCRIPT_DIR/伪分布"

if [ ! -d "$SRC_ROOT" ]; then
  echo "错误: 备份目录不存在: $SRC_ROOT"
  exit 1
fi

echo "将从备份目录恢复以下文件到系统位置："
find "$SRC_ROOT" -type f | sed -e "s#^$SRC_ROOT##" -e 's#^#  #' | sed -n '1,200p'

read -p "确认要覆盖系统上的这些文件吗？(yes/NO) " yn
if [ "$yn" != "yes" ]; then
  echo "已取消恢复。"
  exit 0
fi

timestamp="$(date +%s)"
# 遍历并恢复
find "$SRC_ROOT" -type f | while IFS= read -r f; do
  rel="${f#$SRC_ROOT}"
  dst="$rel"
  dst_dir="$(dirname "$dst")"
  echo "恢复: $f -> $dst"
  # 创建目标目录（需要 sudo）
  if [ ! -d "$dst_dir" ]; then
    echo "  创建目标目录 $dst_dir (使用 sudo)"
    sudo mkdir -p "$dst_dir"
  fi
  # 如果目标已存在，先做备份
  if [ -e "$dst" ]; then
    backup="${dst}.bak.${timestamp}"
    echo "  目标已存在，移动到备份: $backup"
    sudo mv "$dst" "$backup"
  fi
  # 复制回去
  sudo cp --preserve=mode,timestamps "$f" "$dst"
  # 尝试恢复所有权与权限参考（如果备份保留了原属主信息）
  sudo chown --reference="$f" "$dst" 2>/dev/null || true
done

echo "恢复完成。建议重启相关服务（HDFS/YARN/ZK/HBase）并检查日志。"