#!/usr/bin/env python3
"""
Job 4 Reducer - Alphabet Distribution
Input:  letter\tcount  (sorted by letter)
Output: letter\ttotal_count
"""

import sys

def main():
    current_letter = None
    current_total = 0

    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        letter, count_str = parts
        try:
            count = int(count_str)
        except ValueError:
            continue

        if letter == current_letter:
            current_total += count
        else:
            if current_letter is not None:
                print(f"{current_letter}\t{current_total}")
            current_letter = letter
            current_total = count

    if current_letter is not None:
        print(f"{current_letter}\t{current_total}")

if __name__ == "__main__":
    main()
