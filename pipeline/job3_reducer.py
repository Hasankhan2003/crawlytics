#!/usr/bin/env python3
"""
Job 3 Reducer - Word Length Statistics
Input:  length\tcount  (sorted by length key)
Output: length\ttotal_count  (total words of that length)
"""

import sys

def main():
    current_length = None
    current_total = 0

    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        length_str, count_str = parts
        try:
            length = int(length_str)
            count = int(count_str)
        except ValueError:
            continue

        if length == current_length:
            current_total += count
        else:
            if current_length is not None:
                print(f"length_{current_length}\t{current_total}")
            current_length = length
            current_total = count

    if current_length is not None:
        print(f"length_{current_length}\t{current_total}")

if __name__ == "__main__":
    main()
