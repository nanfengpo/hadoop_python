# coding:utf8

'''
    补充：统计总共有多少种职业（jobTitle）
'''

from mrjob.job import MRJob
import csv

count = 0

class JobTitleCount(MRJob):
    def mapper(self,_,line):
        row = csv.reader([line]).next()
        yield row[1],1

    def reducer(self, key, values):
        global count
        count+=1

if __name__ == '__main__':
    JobTitleCount.run()
    print count