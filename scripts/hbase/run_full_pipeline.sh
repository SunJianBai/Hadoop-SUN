#!/bin/bash
set -euo pipefail

# Full pipeline: run streaming examples (maps/reduces) then load movie_avg into HBase.
# Usage: ./run_full_pipeline.sh

: ${REPO_ROOT:=$(cd "$(dirname "$0")/../.." && pwd)}
: ${HBASE_HOME:=/usr/local/hbase}
: ${HADOOP_HOME:=/usr/local/hadoop}

echo "1) Run streaming examples (mapreduce)"
chmod +x "$REPO_ROOT/scripts/run_streaming_examples.sh"
"$REPO_ROOT/scripts/run_streaming_examples.sh"

echo
echo "2) Load /user/$USER/output/movie_avg into HBase"
chmod +x "$REPO_ROOT/scripts/hbase/load_movie_avg_to_hbase.sh"
"$REPO_ROOT/scripts/hbase/load_movie_avg_to_hbase.sh" "/user/$USER/output/movie_avg"

echo "Pipeline finished. You can inspect HBase table 'movie_avg' via HBase shell or Master UI."
