
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
        # 【注意】即便是mapper，也有key与value对。只不过这里只有value(line,是输入文本的每一行,是字符串类型)，没有key，因此设置key为_
        '''
        # 方法1：使用line.split()
        pattern = re.compile(r'(\W+)')
        for word in line.split():
            word = re.sub(pattern,r'',word)
            if word.isalpha():
                yield(word.lower(),1)
        '''
        '''
        # 方法2：使用re.split()切分
        pattern = re.compile(r'(\W+)')
        for word in re.split(pattern,line):
            if word.isalpha():
                yield(word.lower(),1)
        '''

        # 方法3：使用re.findall()划分
        pattern = re.compile(r'(\w+)')
        for word in re.findall(pattern,line):
            if word.isalpha():
                yield (word.lower(),1)



    def reducer(self,word,counts):
        # 【注意】counts是generator类型！只能执行一次！因此建议先用list()将其转化为列表。
        s = list(counts)
        print("word:",word)
        print("counts:", s)
        # print("type of counts:",type(counts))
        yield(word,sum(s))

if __name__ == '__main__':
    MRWordCount.run()




