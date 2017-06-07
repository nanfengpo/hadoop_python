#! /usr/bin/python

import sys

# Read each line from stdin
for line in sys.stdin:
    # Get the words in each line
    words = line.split()

    # Generate the count for each word
    for word in words:
        print '{0}\t{1}'.format(word,1)


