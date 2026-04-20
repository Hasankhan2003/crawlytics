#!/usr/bin/env python3
"""
Job 6 Reducer - Final Filtered Ranking
Input:  count\tword  (sorted ascending by Hadoop; we reverse to get descending)
Output: rank\tword\tcount  – filtered list (no stop-words, words >= 3 chars)

Stop words (English function words) are removed here for analytical value.
"""

import sys

# Common English stop words to filter out
STOP_WORDS = {
    "the", "and", "for", "that", "are", "was", "with", "this",
    "from", "not", "but", "they", "have", "had", "you", "all",
    "been", "one", "which", "who", "were", "their", "when",
    "can", "said", "its", "will", "more", "about", "has",
    "than", "into", "your", "some", "him", "what", "out",
    "also", "she", "there", "would", "his", "her", "our",
    "any", "other", "then", "them", "these", "those", "such",
    "after", "new", "over", "time", "may", "way", "could",
    "each", "make", "like", "two", "how", "even", "just",
    "did", "now", "only", "most", "use", "see", "get",
}

MIN_WORD_LENGTH = 3

def main():
    # Collect all records; they arrive sorted ascending by count (zero-padded).
    # We reverse for descending rank.
    records = []
    for line in sys.stdin:
        line = line.rstrip('\n')
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) != 2:
            continue

        count_str, word = parts
        try:
            count = int(count_str.strip())
        except ValueError:
            continue

        records.append((count, word))

    # Sort descending by count
    records.sort(key=lambda x: x[0], reverse=True)

    rank = 1
    print("Rank\tWord\tCount")
    print("-" * 40)
    for count, word in records:
        if len(word) < MIN_WORD_LENGTH:
            continue
        if word.lower() in STOP_WORDS:
            continue
        print(f"{rank}\t{word}\t{count}")
        rank += 1

if __name__ == "__main__":
    main()
