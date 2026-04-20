#!/usr/bin/env python3
"""
Job 5 Reducer - Top-50 Identification
Input:  topn\tword\tcount  (all with same key "topn")
Output: word\tcount  for the top 50 most frequent words

Uses heapq.nlargest which is O(n log k) – efficient even on large inputs.
"""

import sys
import heapq

TOP_N = 50

def main():
    words = []

    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue

        parts = line.split('\t')
        if len(parts) != 3:
            continue

        _key, word, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        words.append((count, word))

    # Get the TOP_N words with the highest counts
    top_words = heapq.nlargest(TOP_N, words, key=lambda x: x[0])

    for count, word in top_words:
        print(f"{word}\t{count}")

if __name__ == "__main__":
    main()
