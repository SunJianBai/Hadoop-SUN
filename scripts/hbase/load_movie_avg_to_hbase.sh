#!/bin/bash
set -euo pipefail

# Load movie average results from HDFS into HBase table `movie_avg` (column family `cf`).
# Usage: ./load_movie_avg_to_hbase.sh [HDFS_INPUT_DIR]
# Example: ./load_movie_avg_to_hbase.sh /user/sun/output/movie_avg

: ${HBASE_HOME:=/usr/local/hbase}
: ${HADOOP_HOME:=/usr/local/hadoop}

HDFS_IN_DIR=${1:-/user/$USER/output/movie_avg}
TMP_DIR=/tmp/hbase_movie_avg_$$
mkdir -p "$TMP_DIR"
TSV_LOCAL="$TMP_DIR/movie_avg.tsv"
HBASE_CMDS="$TMP_DIR/hbase_puts.rb"

echo "Exporting HDFS output from $HDFS_IN_DIR to $TSV_LOCAL..."
$HADOOP_HOME/bin/hdfs dfs -cat "$HDFS_IN_DIR/part-*" > "$TSV_LOCAL"

if [ ! -s "$TSV_LOCAL" ]; then
  echo "No data found at $HDFS_IN_DIR. Exiting." >&2
  exit 1
fi

echo "Preparing HBase commands..."
# Expect lines: movie_id\tavg\tcount
cat > "$HBASE_CMDS" <<'EOF'
disable 'movie_avg' rescue nil
drop 'movie_avg' rescue nil
create 'movie_avg', 'cf'
EOF

awk -F"\t" '{movie=$1; avg=$2; cnt=$3; gsub(/\r/,"",avg); gsub(/\r/,"",cnt); printf("put \"movie_avg\", \"%s\", \"cf:avg\", \"%s\"\n", movie, avg) }' "$TSV_LOCAL" >> "$HBASE_CMDS"

echo "Running HBase shell to load data (this will recreate table 'movie_avg')..."
$HBASE_HOME/bin/hbase shell "$HBASE_CMDS"

echo "Load complete. Sample rows (first 20):"
$HBASE_HOME/bin/hbase shell <<'EOF'
scan 'movie_avg', {LIMIT => 20}
EOF

echo "Cleaning up temporary files: $TMP_DIR"
rm -rf "$TMP_DIR"

echo "Done."
