
# coding:utf-8

'''
本程序的目的：
找出平均年薪最高的十个职业

【注意】这里没法用combiner

'''

from mrjob.job import MRJob
from mrjob.job import MRStep
import csv

class AvgSalary(MRJob):
    def mapper1(self,_,line):
        row = csv.reader([line]).next()

        if row[0] == " Name":
            return
        # row[1]是职业，float(row[5][1:]是年新
        yield row[1],int(float(row[5][1:]))

    def reducer1(self, key, values):
        '''
        key是职业,values是所有该职业的薪水
        '''
        l = list(values)

        # 只输出样本数大于10的职业
        if len(l)>10:
            yield key,sum(l)/len(l)

    def mapper2(self,job,avg):
        # 调换顺序，方便排序
        yield None,(avg,job)

    def reducer2(self,_,values):
        l = list(values)
        # l是列表，里面的每个元素是平均年新和相应的职业
        # 也就是说，有多少个职业，l的长度就为多少
        l.sort()
        for i in l[-10:]:
            yield i[1],i[0]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1,
                   #combiner = self.reducer1,
                   reducer = self.reducer1),
            MRStep(mapper = self.mapper2,
                   reducer = self.reducer2)
        ]

if __name__ == "__main__":
    AvgSalary.run()

