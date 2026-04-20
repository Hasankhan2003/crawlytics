#!/usr/bin/env bash
# ============================================================
# run_pipeline.sh
# Master shell script that runs the full 6-stage MapReduce
# pipeline using hadoop-streaming.jar directly.
# Alternative to driver.py – produces identical results.
# ============================================================
set -euo pipefail

# ── Configuration ───────────────────────────────────────────
HADOOP_HOME="${HADOOP_HOME:-/usr/local/hadoop}"
PIPELINE_DIR="$(cd "$(dirname "$0")" && pwd)"

# Auto-detect streaming jar
STREAMING_JAR=""
for candidate in \
    "${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming"*.jar \
    /usr/local/hadoop-*/share/hadoop/tools/lib/hadoop-streaming*.jar \
    /usr/lib/hadoop-mapreduce/hadoop-streaming*.jar \
    /opt/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar \
    /opt/hadoop-*/share/hadoop/tools/lib/hadoop-streaming*.jar; do
    if [ -f "$candidate" ]; then
        STREAMING_JAR="$candidate"
        break
    fi
done

if [ -z "$STREAMING_JAR" ]; then
    echo "[ERROR] hadoop-streaming*.jar not found."
    echo "        Set HADOOP_STREAMING_JAR env var or update the candidates list."
    exit 1
fi

HDFS_INPUT="/assignment/input"
HDFS_OUT="/assignment/output"
# ────────────────────────────────────────────────────────────

echo "============================================================"
echo "  BDA Assignment 02 – MapReduce Pipeline (Bash Runner)"
echo "  Streaming JAR : $STREAMING_JAR"
echo "  Pipeline dir  : $PIPELINE_DIR"
echo "============================================================"

# Make all Python scripts executable
chmod +x "$PIPELINE_DIR"/job*.py

# Helper: delete HDFS output dir (no error if missing)
hdfs_rm() { hdfs dfs -rm -r -f "$1" || true; }

# Helper: run one streaming job
run_job() {
    local job_num="$1"
    local mapper="$2"
    local reducer="$3"
    local input="$4"
    local output="$5"
    local num_reducers="${6:-1}"

    echo ""
    echo "------------------------------------------------------------"
    echo "  JOB ${job_num}: mapper=${mapper}  reducer=${reducer}"
    echo "  INPUT : ${input}"
    echo "  OUTPUT: ${output}"
    echo "------------------------------------------------------------"

    hdfs_rm "$output"

    hadoop jar "$STREAMING_JAR" \
        -files "${PIPELINE_DIR}/${mapper},${PIPELINE_DIR}/${reducer}" \
        -mapper  "$mapper" \
        -reducer "$reducer" \
        -input   "$input" \
        -output  "$output" \
        -numReduceTasks "$num_reducers"

    if [ $? -ne 0 ]; then
        echo "[FATAL] Job ${job_num} failed. Pipeline aborted."
        exit 1
    fi
    echo "[OK] Job ${job_num} finished → ${output}"
}

# ── Job 1: Text Cleaning ─────────────────────────────────────
run_job 1 job1_mapper.py job1_reducer.py \
    "$HDFS_INPUT" "$HDFS_OUT/job1" 1

# ── Job 2: Word Count Aggregation ────────────────────────────
run_job 2 job2_mapper.py job2_reducer.py \
    "$HDFS_OUT/job1" "$HDFS_OUT/job2" 4

# ── Job 3: Word Length Statistics (reads Job 2 output) ───────
run_job 3 job3_mapper.py job3_reducer.py \
    "$HDFS_OUT/job2" "$HDFS_OUT/job3" 1

# ── Job 4: Alphabet Distribution (reads Job 2 output) ────────
run_job 4 job4_mapper.py job4_reducer.py \
    "$HDFS_OUT/job2" "$HDFS_OUT/job4" 1

# ── Job 5: Top-50 Identification (reads Job 2 output) ────────
run_job 5 job5_mapper.py job5_reducer.py \
    "$HDFS_OUT/job2" "$HDFS_OUT/job5" 1

# ── Job 6: Final Filtered Ranking (reads Job 5 output) ───────
run_job 6 job6_mapper.py job6_reducer.py \
    "$HDFS_OUT/job5" "$HDFS_OUT/job6" 1

echo ""
echo "============================================================"
echo "  ALL 6 JOBS COMPLETED SUCCESSFULLY"
echo "============================================================"
echo ""
echo "Inspect results:"
echo "  hdfs dfs -cat ${HDFS_OUT}/job3/part-00000    # Word Length"
echo "  hdfs dfs -cat ${HDFS_OUT}/job4/part-00000    # Alphabet Dist."
echo "  hdfs dfs -cat ${HDFS_OUT}/job5/part-00000    # Top-50 Words"
echo "  hdfs dfs -cat ${HDFS_OUT}/job6/part-00000    # Final Ranking"
