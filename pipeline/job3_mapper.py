#!/usr/bin/env python3
"""
Job 3 Mapper - Word Length Statistics
Input:  word\tcount  (output from Job 2 – aggregated word counts)
Output: length\tcount
        e.g.  5\t100  (word of length 5 appeared 100 times total)
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

        word_length = len(word)
        if word_length > 0:
            print(f"{word_length}\t{count}")

if __name__ == "__main__":
    main()
