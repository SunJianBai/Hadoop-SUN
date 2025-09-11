#!/usr/bin/env bash
set -euo pipefail

# Show cluster status
# - Run this on master. SSH into workers manually to run the printed commands.

WORKERS=("kxg@worker1" "xwb@worker2")

echo "== Local (master) jps =="
jps || true

echo
echo "== Worker: run these commands on each worker (or SSH and run locally) =="
for h in "${WORKERS[@]}"; do
	echo "ssh ${h}  jps"
done

echo
echo "Web UIs (master):"
echo "NameNode: http://localhost:9870"
echo "ResourceManager: http://localhost:8088"
echo "HBase Master: http://localhost:16010"
echo "ZooKeeper client port: 2181"

