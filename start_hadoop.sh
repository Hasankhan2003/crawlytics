#!/usr/bin/env bash
# ============================================================
# start_hadoop.sh
# Starts Hadoop DFS and YARN daemons, then creates the HDFS
# input directory and uploads the 10 WET files.
# ============================================================
set -euo pipefail

# ── Configurable paths ──────────────────────────────────────
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
LOCAL_DATA_DIR="$(cd "$(dirname "$0")/.." && pwd)/downloaded_wet_files"
HDFS_INPUT="/assignment/input"
# ────────────────────────────────────────────────────────────

echo "============================================================"
echo "  BDA Assignment 02 – Hadoop Startup & Data Ingestion"
echo "  HADOOP_HOME : $HADOOP_HOME"
echo "  Local data  : $LOCAL_DATA_DIR"
echo "  HDFS input  : $HDFS_INPUT"
echo "============================================================"

# 1. Format NameNode only if it has never been formatted
HADOOP_DATA="${HADOOP_HOME}/data"
if [ ! -d "${HADOOP_DATA}/nameNode/current" ]; then
    echo "[INFO] NameNode not formatted – formatting now..."
    "${HADOOP_HOME}/bin/hdfs" namenode -format -nonInteractive
fi

# 2. Start DFS (NameNode + DataNode)
echo "[INFO] Starting HDFS daemons..."
"${HADOOP_HOME}/sbin/start-dfs.sh"

# 3. Start YARN (ResourceManager + NodeManager)
echo "[INFO] Starting YARN daemons..."
"${HADOOP_HOME}/sbin/start-yarn.sh"

# 4. Wait a moment for services to be ready
sleep 5

# 5. Verify daemons are running
echo "[INFO] Running JVMs:"
jps

# 6. Create the HDFS input directory (ok if already exists)
echo "[INFO] Creating HDFS directory: $HDFS_INPUT"
"${HADOOP_HOME}/bin/hdfs" dfs -mkdir -p "$HDFS_INPUT"

# 7. Upload the 10 WET files
echo "[INFO] Uploading files from $LOCAL_DATA_DIR → $HDFS_INPUT"
for f in "$LOCAL_DATA_DIR"/data-*; do
    fname=$(basename "$f")
    echo "  Uploading $fname ..."
    "${HADOOP_HOME}/bin/hdfs" dfs -put -f "$f" "$HDFS_INPUT/$fname"
done

# 8. Verify upload
echo ""
echo "[INFO] Files now on HDFS:"
"${HADOOP_HOME}/bin/hdfs" dfs -ls "$HDFS_INPUT"

echo ""
echo "============================================================"
echo "  Hadoop is running. Run the pipeline with:"
echo "    python3 pipeline/driver.py"
echo "  OR:"
echo "    bash pipeline/run_pipeline.sh"
echo "============================================================"
