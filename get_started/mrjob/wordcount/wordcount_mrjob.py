
# coding:utf-8

'''
# 使用nltk处理文本，可以本地运行，但不能在hadoop上运行，因为nltk需要本地数据
from mrjob.job import MRJob

import nltk

nltk.data.path.append('/home/nanfengpo/Documents/storm_get_started/twitter/nltk_data')

class MRWordCount(MRJob):
    def mapper(self,_,line):
        for word in nltk.word_tokenize(line):
            if word.isalpha():
                yield(word.lower(),1)

    def reducer(self,word,counts):
        yield(word,sum(counts))

if __name__ == '__main__':
    MRWordCount.run()
'''


from mrjob.job import MRJob
import re

class MRWordCount(MRJob):
    def mapper(self,_,line):
        pattern = re.compile(r'(\W+)')
        for word in line.split():
            word = re.sub(pattern,r'',word)
            if word.isalpha():
                yield(word.lower(),1)

    def reducer(self,word,counts):
        yield(word,sum(counts))

if __name__ == '__main__':
    MRWordCount.run()




