
# coding:utf-8
'''
【级别】
    Easy
【目的】
    就是wordcount,加上了一个combiner

'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import re

WORD_REGEXP = re.compile(r"[\w']+")

class MRWordFrequencyCount(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1
    
    def combiner(self, key, values):
        yield key, sum(values)
    
    def reducer(self, key, values):
        yield key, sum(values)
        

if __name__ == '__main__':
    MRWordFrequencyCount.run()