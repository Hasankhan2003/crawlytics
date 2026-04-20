#!/usr/bin/env python3
"""
Job 5 Mapper - Top-N Pass-Through
Input:  word\tcount  (output from Job 2)
Output: word\tcount  (unchanged; reducer selects Top-50)

We emit a constant key "topn" so that ALL records land in ONE reducer,
allowing the reducer to globally rank all words.
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

        # Emit with a fixed key so all words go to a single reducer
        print(f"topn\t{word}\t{count}")

if __name__ == "__main__":
    main()
