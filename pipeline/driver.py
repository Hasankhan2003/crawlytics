#!/usr/bin/env python3
"""
Pipeline Driver
Executes all 6 Hadoop MapReduce jobs sequentially using hadoop-streaming.jar.
Each job's output becomes the next job's input.
"""

import subprocess
import sys
import os
import glob

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION  –  edit these paths if your Hadoop installation differs
# ─────────────────────────────────────────────────────────────────────────────

# Auto-detect streaming jar; falls back to the most common location
def find_streaming_jar():
    candidates = glob.glob(
        "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar"
    ) + glob.glob(
        "/usr/local/hadoop-*/share/hadoop/tools/lib/hadoop-streaming*.jar"
    ) + glob.glob(
        "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar"
    ) + glob.glob(
        "/opt/hadoop-*/share/hadoop/tools/lib/hadoop-streaming*.jar"
    ) + glob.glob(
        "/usr/lib/hadoop-mapreduce/hadoop-streaming*.jar"
    ) + glob.glob(
        "/home/*/hadoop/share/hadoop/tools/lib/hadoop-streaming*.jar"
    )
    if candidates:
        return candidates[0]
    # Last-resort: let the user override with an environment variable
    env_jar = os.environ.get("HADOOP_STREAMING_JAR", "")
    if env_jar and os.path.exists(env_jar):
        return env_jar
    raise FileNotFoundError(
        "hadoop-streaming*.jar not found. Set HADOOP_STREAMING_JAR env var."
    )

STREAMING_JAR = find_streaming_jar()

# Absolute local path to the pipeline directory (where this script lives)
PIPELINE_DIR = os.path.dirname(os.path.abspath(__file__))

# HDFS base paths
HDFS_INPUT   = "/assignment/input"     # where the raw WET files live
HDFS_OUTPUT  = "/assignment/output"    # root for all intermediate outputs

# ─────────────────────────────────────────────────────────────────────────────

def hdfs_rm_rf(hdfs_path: str):
    """Remove an HDFS directory/file recursively (ignore error if not found)."""
    subprocess.run(
        ["hdfs", "dfs", "-rm", "-r", "-f", hdfs_path],
        check=False,
        stderr=subprocess.DEVNULL,
    )


def run_job(
    job_num: int,
    mapper: str,
    reducer: str,
    input_path: str,
    output_path: str,
    num_reducers: int = 1,
    extra_args: list = None,
):
    """
    Run a single hadoop-streaming job.

    Parameters
    ----------
    job_num      : 1-based job number (for logging)
    mapper       : basename of the mapper script in PIPELINE_DIR
    reducer      : basename of the reducer script in PIPELINE_DIR
    input_path   : HDFS input path
    output_path  : HDFS output path (will be deleted before running)
    num_reducers : number of Hadoop reducers
    extra_args   : additional hadoop streaming arguments
    """
    mapper_path  = os.path.join(PIPELINE_DIR, mapper)
    reducer_path = os.path.join(PIPELINE_DIR, reducer)

    print(f"\n{'='*70}")
    print(f"  JOB {job_num} | mapper={mapper}  reducer={reducer}")
    print(f"  INPUT : {input_path}")
    print(f"  OUTPUT: {output_path}")
    print(f"{'='*70}\n")

    # Clean up any previous output
    hdfs_rm_rf(output_path)

    cmd = [
        "hadoop", "jar", STREAMING_JAR,
        "-files",   f"{mapper_path},{reducer_path}",
        "-mapper",  mapper,
        "-reducer", reducer,
        "-input",   input_path,
        "-output",  output_path,
        "-numReduceTasks", str(num_reducers),
    ]

    if extra_args:
        cmd.extend(extra_args)

    result = subprocess.run(cmd, text=True)

    if result.returncode != 0:
        print(f"\n[FATAL] Job {job_num} FAILED (exit code {result.returncode})")
        print("Pipeline aborted.")
        sys.exit(result.returncode)

    print(f"\n[OK] Job {job_num} completed successfully → {output_path}")


def main():
    print("=" * 70)
    print("  BDA Assignment 02 – Hadoop MapReduce Pipeline")
    print(f"  Streaming JAR : {STREAMING_JAR}")
    print(f"  Pipeline dir  : {PIPELINE_DIR}")
    print("=" * 70)

    # ── Job 1: Text Cleaning ────────────────────────────────────────────────
    # Input : raw WET files in HDFS_INPUT
    # Output: word\t1  (cleaned tokens)
    run_job(
        job_num      = 1,
        mapper       = "job1_mapper.py",
        reducer      = "job1_reducer.py",
        input_path   = HDFS_INPUT,
        output_path  = f"{HDFS_OUTPUT}/job1",
        num_reducers = 1,
    )

    # ── Job 2: Word Count Aggregation ───────────────────────────────────────
    # Input : word\t1  (Job 1 output)
    # Output: word\tcount
    run_job(
        job_num      = 2,
        mapper       = "job2_mapper.py",
        reducer      = "job2_reducer.py",
        input_path   = f"{HDFS_OUTPUT}/job1",
        output_path  = f"{HDFS_OUTPUT}/job2",
        num_reducers = 4,   # parallelise the aggregation
    )

    # ── Job 3: Word Length Statistics ───────────────────────────────────────
    # Input : word\tcount  (Job 2 output)
    # Output: length_N\ttotal_count
    run_job(
        job_num      = 3,
        mapper       = "job3_mapper.py",
        reducer      = "job3_reducer.py",
        input_path   = f"{HDFS_OUTPUT}/job2",
        output_path  = f"{HDFS_OUTPUT}/job3",
        num_reducers = 1,
    )

    # ── Job 4: Alphabet Distribution ────────────────────────────────────────
    # Input : word\tcount  (Job 2 output)
    # Output: letter\ttotal_count
    run_job(
        job_num      = 4,
        mapper       = "job4_mapper.py",
        reducer      = "job4_reducer.py",
        input_path   = f"{HDFS_OUTPUT}/job2",
        output_path  = f"{HDFS_OUTPUT}/job4",
        num_reducers = 1,
    )

    # ── Job 5: Top-50 Identification ────────────────────────────────────────
    # Input : word\tcount  (Job 2 output)
    # Output: word\tcount  (top 50 only)
    # Uses 1 reducer so global ranking is possible.
    # We increase the vmem ratio because loading 6.4 million records 
    # into heapq uses significant virtual memory relative to physical memory.
    run_job(
        job_num      = 5,
        mapper       = "job5_mapper.py",
        reducer      = "job5_reducer.py",
        input_path   = f"{HDFS_OUTPUT}/job2",
        output_path  = f"{HDFS_OUTPUT}/job5",
        num_reducers = 1,
        extra_args   = [
            "-D", "yarn.nodemanager.vmem-check-enabled=false",
            "-D", "mapreduce.reduce.memory.mb=2048",
            "-D", "mapreduce.reduce.java.opts=-Xmx1638m"
        ]
    )

    # ── Job 6: Final Filtered Ranking ───────────────────────────────────────
    # Input : word\tcount  (Job 5 output)
    # Output: rank\tword\tcount  (stop-words removed, min length 3)
    run_job(
        job_num      = 6,
        mapper       = "job6_mapper.py",
        reducer      = "job6_reducer.py",
        input_path   = f"{HDFS_OUTPUT}/job5",
        output_path  = f"{HDFS_OUTPUT}/job6",
        num_reducers = 1,
    )

    print("\n" + "=" * 70)
    print("  ALL JOBS COMPLETED SUCCESSFULLY!")
    print(f"  Final output : {HDFS_OUTPUT}/job6")
    print("=" * 70)
    print("\nRun the following commands to inspect results:")
    print(f"  hdfs dfs -cat {HDFS_OUTPUT}/job3/part-00000   # Word Length Stats")
    print(f"  hdfs dfs -cat {HDFS_OUTPUT}/job4/part-00000   # Alphabet Distribution")
    print(f"  hdfs dfs -cat {HDFS_OUTPUT}/job6/part-00000   # Final Ranked List")


if __name__ == "__main__":
    main()
