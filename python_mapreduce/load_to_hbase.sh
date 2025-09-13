#!/usr/bin/env bash
set -euo pipefail

HDFS_OUT_DIR="$1"  # pass HDFS output directory as first arg
TABLE_NAME="InvertedIndexTable"

tmpfile=$(mktemp)
hdfs dfs -cat ${HDFS_OUT_DIR}/part-* > "$tmpfile"

echo "create '$TABLE_NAME', 'info'" | hbase shell -n || true

while IFS=$'\t' read -r word filelist; do
  # filelist like file1:3;file2:5;
  # build put commands
  echo "put '$TABLE_NAME', '$word', 'info:index', '$filelist'" >> /tmp/hbase_puts.txt
done < "$tmpfile"

hbase shell /tmp/hbase_puts.txt
rm -f /tmp/hbase_puts.txt
rm -f "$tmpfile"

echo "Loaded reducer output to HBase table $TABLE_NAME"
