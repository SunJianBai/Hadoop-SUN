// ...existing code...
#!/usr/bin/env bash
# 通过 sshpass + sudo 密码管道 将本地已修改的配置文件分发到两个 worker 并移动到目标目录
# 注意：脚本会把文件先上传到 /tmp，然后在远端用 sudo mv 移动到最终路径
# 请保证本地文件路径正确；如有不同请修改相应路径变量

set -euo pipefail

# 目标主机与对应 sudo 密码（按顺序对应）
HOSTS=("kxg@worker1" "xwb@worker2")
PWDS=("123" "6")

# 本地文件列表（源 -> 远端 /tmp 名称 -> 目标路径）
declare -a FILES_SRC=(
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

declare -a FILES_TMPNAME=(
  "hosts"
  "core-site.xml"
  "hdfs-site.xml"
  "yarn-site.xml"
  "mapred-site.xml"
  "workers"
  "zoo.cfg"
  "hbase-site.xml"
  "hbase-env.sh"
  "regionservers"
)

declare -a FILES_DST=(
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

# 检查 sshpass 是否可用
if ! command -v sshpass >/dev/null 2>&1; then
  echo "错误: 未检测到 sshpass。请先安装：sudo apt-get install -y sshpass"
  exit 1
fi

# 逐主机分发
for idx in "${!HOSTS[@]}"; do
  H="${HOSTS[$idx]}"
  P="${PWDS[$idx]}"
  echo "==> 目标: $H"

  for i in "${!FILES_SRC[@]}"; do
    SRC="${FILES_SRC[$i]}"
    TMPNAME="${FILES_TMPNAME[$i]}"
    DST="${FILES_DST[$i]}"

    if [ ! -f "$SRC" ]; then
      echo "警告: 本地源文件不存在，跳过: $SRC"
      continue
    fi

    echo "  上传 $SRC -> $H:/tmp/$TMPNAME"
    sshpass -p "$P" scp -o StrictHostKeyChecking=no "$SRC" "$H:/tmp/$TMPNAME"

    echo "  在远端移动 /tmp/$TMPNAME -> $DST (通过 sudo)"
    # 使用 echo 密码管道给 sudo
    sshpass -p "$P" ssh -o StrictHostKeyChecking=no "$H" \
      "echo '$P' | sudo -S mv /tmp/$TMPNAME $DST && sudo chown root:root $DST || (echo '远端移动失败: $DST' && exit 1)"
  done

  echo "完成 $H 的分发"
done

echo "全部分发完成。建议在 master 上按顺序重启 HDFS/YARN/ZK/HBase 并检查日志。"
# ...existing code...