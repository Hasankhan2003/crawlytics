#!/usr/bin/env python3
"""
Job 2 Reducer - Word Count Aggregation
Input:  word\t1  (sorted by key from Hadoop's shuffle)
Output: word\tcount
"""

import sys

def main():
    current_word = None
    current_count = 0

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

        if word == current_word:
            current_count += count
        else:
            if current_word is not None:
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Emit the last word
    if current_word is not None:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    main()
