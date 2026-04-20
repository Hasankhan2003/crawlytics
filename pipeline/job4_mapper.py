#!/usr/bin/env python3
"""
Job 4 Mapper - Alphabet Distribution
Input:  word\tcount  (output from Job 2)
Output: first_letter\tcount
Only considers words starting with a-z.
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

        if word and word[0].isalpha():
            first_letter = word[0].lower()
            print(f"{first_letter}\t{count}")

if __name__ == "__main__":
    main()
