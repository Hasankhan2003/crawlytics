#!/usr/bin/env python3
"""
Job 1 Reducer - Text Cleaning (pass-through)
The mapper already emits word\t1 pairs.  The reducer is an identity
pass-through so that Job 2 can perform the actual count aggregation.
This is intentional: keeping cleaning and aggregation in separate jobs
makes the pipeline easier to debug and re-run independently.
"""

import sys

def main():
    for line in sys.stdin:
        line = line.rstrip('\n')
        if line:
            print(line)

if __name__ == "__main__":
    main()
