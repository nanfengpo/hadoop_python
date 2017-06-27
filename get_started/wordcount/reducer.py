# coding:utf-8
#! /usr/bin/python

#【注意】直接使用hadoop streaming与使用mrjob的不同
# 1. streaming中无论是mapper还是reducer都需要从sys.stdin中读取
# 2. streaming中reducer的sys.stdin的每一行是按照key来排序的，每一行都是一对key和value，且需要使用.split('\t')分隔开来。
# 3. streaming中reducer得到的value是单个值，而mrjob是一个generator

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