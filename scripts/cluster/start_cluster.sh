#!/usr/bin/env bash
set -euo pipefail

# Start cluster (master-run script)
# - Run this on the master node.
# - Worker nodes should be started by you via `ssh kxg@worker1` / `ssh xwb@worker2` and running the printed commands.

WORKERS=("kxg@worker1" "xwb@worker2")

echo "STEP 1) Start HDFS and YARN on master"
echo "=== start-dfs.sh ==="
start-dfs.sh

echo "=== start-yarn.sh ==="
start-yarn.sh

echo
echo "STEP 2) Start ZooKeeper on workers"
echo "Run the following on each worker (or run after you SSH into each):"
for h in "${WORKERS[@]}"; do
	echo "  ssh ${h}  zkServer.sh start"
done

echo
read -p "Press ENTER after you have started ZooKeeper on all workers to continue and start HBase on master (Ctrl-C to abort): " _

echo
echo "STEP 3) Start HBase on master"
echo "=== start-hbase.sh ==="
start-hbase.sh

echo
echo "Start sequence finished. Run ./status_cluster.sh to check status."

