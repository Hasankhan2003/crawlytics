# BDA Assignment 02: Hadoop MapReduce Pipeline

**Name:** Hasan  
**ID:** i221945  
**Course:** Big Data Analytics  

## Overview
This repository contains a complete 6-stage Hadoop MapReduce pipeline implemented using Python's Hadoop Streaming API. It processes raw Common Crawl web archive data (WET format) to clean the text, calculate word counts, generate word length and alphabet distributions, and ultimately produce a globally ranked list of the 50 most frequent meaningful words (excluding stop-words).

## Directory Structure
- `download_common_crawl.py` - Fetches the initial batch of random WET files.
- `start_hadoop.sh` - Helper script to start HDFS/YARN and format HDFS if necessary.
- `pipeline/` - Contains all MapReduce jobs and execution scripts.
  - `driver.py` - Main orchestrator script to run Jobs 1-6 sequentially.
  - `run_pipeline.sh` - Alternative Bash runner for the pipeline.
  - `job1_mapper.py` / `job1_reducer.py` - **Job 1 (Text Cleaning):** Strips WARC headers, punctuation, filters out non-ASCII, and emits `word \t 1`.
  - `job2_mapper.py` / `job2_reducer.py` - **Job 2 (Word Count):** Aggregates counts.
  - `job3_mapper.py` / `job3_reducer.py` - **Job 3 (Word Lengths):** Emits occurrences by word length.
  - `job4_mapper.py` / `job4_reducer.py` - **Job 4 (Alphabet Distribution):** Emits occurrences by starting letter.
  - `job5_mapper.py` / `job5_reducer.py` - **Job 5 (Top-50):** Routes all unique word pairs to a single reducer to find the Top-50 list using a priority queue.
  - `job6_mapper.py` / `job6_reducer.py` - **Job 6 (Filter):** Filters out stop-words and outputs final rank.
- `Pipeline_Design_Report.md` - Technical overview of the MapReduce data flow.

## Prerequisites
- Hadoop `2.x` or `3.x` cluster (configured locally or distributed).
- Python 3+

## Execution Instructions

### 1. Start Hadoop cluster
Ensure Hadoop's NameNode, DataNode, ResourceManager, and NodeManager are running:
```bash
/usr/local/hadoop-2.10.2/sbin/start-dfs.sh
/usr/local/hadoop-2.10.2/sbin/start-yarn.sh
jps
```

### 2. Download the Raw Dataset
Run the data ingestion script to download the WET files (configured to fetch 20 random segments):
```bash
python3 download_common_crawl.py
```

### 3. Upload Data to HDFS
```bash
hdfs dfs -mkdir -p /assignment/input
for f in downloaded_wet_files/data-*; do
  hdfs dfs -put -f "$f" "/assignment/input/$(basename "$f")"
done
```

### 4. Run the Pipeline
The `driver.py` script automatically locates the `hadoop-streaming.jar` and manages HDFS output paths for the sequential execution of Jobs 1 through 6.
```bash
python3 pipeline/driver.py
```
*(Note: Job 5 requires increased Virtual Memory. To run it manually if YARN memory limits are restrictive:)*
```bash
hadoop jar /usr/local/hadoop-2.10.2/share/hadoop/tools/lib/hadoop-streaming-2.10.2.jar \
    -D mapreduce.map.memory.mb=1024 \
    -D mapreduce.reduce.memory.mb=3072 \
    -D yarn.nodemanager.vmem-check-enabled=false \
    -D yarn.nodemanager.vmem-pmem-ratio=5 \
    -files pipeline/job5_mapper.py,pipeline/job5_reducer.py \
    -mapper "python3 job5_mapper.py" \
    -reducer "python3 job5_reducer.py" \
    -input /assignment/output/job2 \
    -output /assignment/output/job5 \
    -numReduceTasks 1
```

### 5. View the Results
Retrieve the final results directly from HDFS:

**Job 3: Word Length Statistics**
```bash
hdfs dfs -cat /assignment/output/job3/part-00000 | head -n 25
```

**Job 4: Alphabet Starts**
```bash
hdfs dfs -cat /assignment/output/job4/part-00000 | head -n 30
```

**Job 6: The Final Top-50 Ranking (Meaningful Words)**
```bash
hdfs dfs -cat /assignment/output/job6/part-00000 | head -n 25
```
