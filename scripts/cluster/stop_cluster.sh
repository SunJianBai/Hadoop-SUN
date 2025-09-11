#!/usr/bin/env bash
set -euo pipefail

# Stop cluster (master-run script)
# - Run this on the master node.
# - Worker nodes should be stopped by you via `ssh kxg@worker1` / `ssh xwb@worker2` and running the printed commands.

WORKERS=("kxg@worker1" "xwb@worker2")

echo "STEP 1) Stop HBase on master"
echo "=== stop-hbase.sh ==="
stop-hbase.sh || true

echo
echo "STEP 2) Stop YARN on master"
echo "=== stop-yarn.sh ==="
stop-yarn.sh || true

echo
echo "STEP 3) Stop HDFS on master"
echo "=== stop-dfs.sh ==="
stop-dfs.sh || true

echo
echo "STEP 4) Stop ZooKeeper on workers"
echo "Run the following on each worker (or run after you SSH into each):"
for h in "${WORKERS[@]}"; do
	echo "  ssh ${h}  zkServer.sh stop"
done

echo
echo "Stop sequence finished."

