# coding:utf-8

'''
【级别】
    Easy

【目的】
    得到评分的分值计数，即1分有多少人打，2分有多少人打
    
【输入文件】
  ./data/ml-100k/u.data 
  
'''

from mrjob.job import MRJob

class MRRatingCounter(MRJob):
  def mapper(self, key, line):
    (userID, movieID, rating, timestamp) = line.split('\t')
    yield rating, 1
  
  def reducer(self, rating, occurences):
    yield rating, sum(occurences)
    
if __name__ == '__main__':
  MRRatingCounter.run()
  