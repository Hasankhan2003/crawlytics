#!/usr/bin/env python3
"""
Job 1 Mapper - Text Cleaning
Reads raw WET (WARC/1.0) files, strips the WARC header block,
then emits one lowercase word per line (tab-separated with count 1).

WET format per record:
  WARC/1.0
  WARC-Type: conversion
  ...header fields...
  Content-Length: NNN
  <blank line>
  <body text lines>
  <blank line>  <-- record separator
"""

import sys
import re
import string

# Prefixes that mark WARC header lines (case-sensitive as per spec)
WARC_HEADER_PREFIXES = (
    "WARC/",
    "WARC-",
    "Content-Type:",
    "Content-Length:",
    "Software-Info:",
    "Extracted-Date:",
    "robots:",
    "isPartOf:",
    "operator:",
    "description:",
    "publisher:",
)

# Punctuation translation table: removes all punctuation characters
PUNCT_TABLE = str.maketrans("", "", string.punctuation)

def is_warc_header_line(line: str) -> bool:
    """Return True if the line is a WARC meta-header line to be skipped."""
    stripped = line.strip()
    if not stripped:
        return False  # blank lines are record separators, not headers
    for prefix in WARC_HEADER_PREFIXES:
        if stripped.startswith(prefix):
            return True
    return False

def process_line(line: str):
    """
    Yield (word, 1) pairs from a cleaned body-text line.
    Returns nothing for WARC header lines or blank lines.
    """
    if is_warc_header_line(line):
        return

    # Remove punctuation, lowercase, split
    cleaned = line.strip().translate(PUNCT_TABLE).lower()
    for word in cleaned.split():
        # Keep only ASCII alphabetic words (skip numbers, URLs, non-Latin scripts)
        if word.isalpha() and word.isascii():
            print(f"{word}\t1")

def main():
    for raw_line in sys.stdin:
        # Hadoop streaming delivers bytes decoded as utf-8;
        # use errors='replace' to avoid crashing on mojibake
        try:
            line = raw_line
            process_line(line)
        except Exception:
            # Never let a single bad line crash the whole mapper
            continue

if __name__ == "__main__":
    main()
