#!/usr/bin/env python3
"""
Job 6 Mapper - Final Filtered Ranking (swap key for sort)
Input:  word\tcount  (output from Job 5 – top 50 words)
Output: count\tword  (swap so Hadoop sorts by count descending)

We zero-pad the count so lexicographic sort equals numeric sort.
"""

import sys

def main():
    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        word, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        # Zero-pad to 15 digits for correct lexicographic integer sort
        print(f"{count:015d}\t{word}")

if __name__ == "__main__":
    main()
