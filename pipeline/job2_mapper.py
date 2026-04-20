#!/usr/bin/env python3
"""
Job 2 Mapper - Word Count (pass-through)
Input: word\t1  (output from Job 1)
Output: word\t1  (unchanged; Hadoop's shuffle+sort groups by word)
"""

import sys

def main():
    for line in sys.stdin:
        line = line.rstrip('\n')
        if line:
            print(line)

if __name__ == "__main__":
    main()
