# BDA Assignment 02: Hadoop MapReduce Pipeline Design Report
**Name:** Hasan
**Course:** Big Data Analytics
**ID:** i221945

## Executive Summary
This report outlines the architecture and design decisions for a 6-stage Hadoop MapReduce pipeline built using Python Streaming. The objective of the pipeline is to process raw Common Crawl web data (WET files), clean the text, and perform various statistical analyses, including word lengths, alphabet distribution, and a filtered Top-50 ranking.

## Data Ingestion & Pre-processing
A Python script (`download_common_crawl.py`) was used to download a random sampling of 20 compressed WET files (approx. 4.4 GB uncompressed) from the Common Crawl dataset. These files were uploaded to the Hadoop Distributed File System (HDFS) under the `/assignment/input` directory to serve as the raw input for the MapReduce pipeline. 

## Pipeline Orchestration
A Python driver script (`driver.py`) orchestrates the sequential execution of the 6 MapReduce jobs using the `subprocess` module to interface with the Hadoop Streaming JAR. The driver handles intermediate HDFS path management and dynamically injects Hadoop configuration parameters (such as `mapreduce.reduce.memory.mb`) when specific jobs require increased resources.

## MapReduce Job Design

### Job 1: Text Cleaning
*   **Input:** Raw WET files from HDFS (`/assignment/input`)
*   **Mapper:** Parses the WET file format line-by-line. It explicitly identifies and discards WARC header metadata (`WARC/1.0`, `WARC-Type`, etc.). The remaining content is converted to lowercase, stripped of all punctuation using Python's `string.punctuation`, and split into individual tokens. Crucially, the mapper enforces an `isalpha() and isascii()` filter to aggressively discard numbers, URLs, and non-Latin multilingual characters, emitting valid English words as `<word> \t 1`.
*   **Reducer:** Acts as an identity pass-through, emitting the exact `<word> \t 1` pairs to HDFS.

### Job 2: Word Count Aggregation
*   **Input:** Output of Job 1
*   **Mapper:** Identity pass-through.
*   **Reducer:** Standard WordCount logic. It aggregates the `1`s for each unique word, emitting the global master dictionary: `<word> \t <total_count>`.

*(Note: Jobs 3, 4, and 5 represent parallel analyses. Therefore, all three of these jobs take the output of Job 2 as their direct input).*

### Job 3: Word Length Statistics
*   **Input:** Output of Job 2 (`<word> \t <count>`)
*   **Mapper:** Calculates the length of the input word and emits `<length_N> \t <count>`.
*   **Reducer:** Aggregates occurrences by length, emitting the total number of words found for each specific word length.

### Job 4: Alphabet Distribution
*   **Input:** Output of Job 2 (`<word> \t <count>`)
*   **Mapper:** Extracts the first character of the word and emits `<first_letter> \t <count>`.
*   **Reducer:** Aggregates occurrences by starting letter, emitting the total distribution of words across the A-Z alphabet.

### Job 5: Top-50 Identification
*   **Input:** Output of Job 2 (`<word> \t <count>`)
*   **Mapper:** A critical design step. The mapper reads all 6.4 million unique word/count pairs and emits them using a static, hardcoded key (`"topn" \t <word_count_json>`).
*   **Reducer (Single Instance):** Because all records are mapped to the exact same static key (`topn`), Hadoop routes the entire dataset to a single Reducer instance. The Reducer uses Python's `heapq.nlargest` memory-efficient priority queue algorithm to sort the massive stream of data dynamically, outputting the Top 50 absolute most frequent words as `<word> \t <count>`. 
    *   *System Tuning:* This single-reducer bottleneck required increasing the `mapreduce.reduce.memory.mb` to `2048` and the `vmem-pmem-ratio` limit to prevent YARN `Container killed` virtual memory errors during the `heapq` population.

### Job 6: Final Filtered Ranking
*   **Input:** Output of Job 5 (Top 50 raw words)
*   **Mapper:** Swaps the key-value pair to `<count> \t <word>`. To ensure Hadoop's underlying MapReduce string-sorting algorithm accurately sorts the integers from highest to lowest, the count is zero-padded (e.g., `000000100 \t word`).
*   **Reducer:** Reverses the sort to descending order. The reducer maintains a hardcoded set of common English "stop words" (e.g., *the, and, of, to*). It iterates through the descending list, aggressively discarding any word that exists in the stop-word list or is shorter than 3 characters. It assigns a numerical rank to the surviving contextual words and outputs the final formatted list: `Rank \t Word \t Count`.

## Implementation Challenges & Solutions
1.  **HDFS Space Exhaustion:** Processing 20 WET files (approx 4.4 GB uncompressed) generated over 25 GB of intermediate MapReduce spills during Job 1. This originally triggered a `No space left on device` error from the YARN NodeManager because the disk utilization crossed the 90% threshold. This was resolved by cleaning corrupted HDFS namespaces and formatting the local NameNode.
2.  **Streaming JAR Detection:** The pipeline required robust auto-discovery of the `hadoop-streaming.jar` path across different potential Hadoop `2.x` and `3.x` cluster installations, which was solved by implementing dynamic glob pathing in the driver script.
3.  **Reducer Memory Limits:** Job 5's global ranking requirement necessitated a single reducer, forcing all 6.4 million unique dictionary entries into one container's memory footprint. This triggered a `Container is running beyond virtual memory limits` application failure. This was solved by modifying the driver to pass explicit JVM heap and YARN vmem ratio override parameters directly into the Job 5 runtime execution.
