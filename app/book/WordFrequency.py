
# coding:utf-8
'''
【级别】
    simple
【目的】
    就是wordcount,求出每个单词的出现次数
    
'''
from mrjob.job import MRJob

class MRWordFrequency(MRJob):

    def mapper(self, _, line):
        words = line.split()
        for word in words:
            yield word.lower(), 1

    def reducer(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    MRWordFrequency.run()
