#! /usr/bin/python

import sys

curr_word = None
curr_count = 0

# Process each key-value pait from the current line
for line in sys.stdin:
    word, count = line.split('\t')

    count = int(count)

    if word == curr_word:
        curr_count += count
    else:
        if curr_word:
            print '{0}\t{1}'.format(curr_word,curr_count)
        curr_word = word
        curr_count = count

# Output the count for the last word
if curr_word == word:
    print '{0}\t{1}'.format(curr_word,curr_count)