
# coding:utf-8
'''
【级别】
    simple
【目的】
    就是wordcount,求出每个单词的出现次数
【改进】
    使用正则表达式

'''

from mrjob.job import MRJob
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        words = WORD_REGEXP.findall(line)
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequency.run()
