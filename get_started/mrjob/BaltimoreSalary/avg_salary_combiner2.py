
# coding:utf-8

'''
【目的】
    本程序和avg_salary_combiner.py的不同在于mapper1输出的values改变了，只有年薪这一个数字。
    事实证明本程序能正常运行。这说明combiner的输出不必和mapper的输出一致
【运行】
    $ python avg_salary_combiner2.py Baltimore_City_Employee_Salaries_FY2014.csv >output2.txt --jobconf mapreduce.job.maps=3
注意：默认mapper任务(mapper task)数量为2，可通过--jobconf mapreduce.job.maps来指定数量


'''

from mrjob.job import MRJob
from mrjob.job import MRStep
import csv

class AvgSalary(MRJob):
    def mapper1(self,_,line):
        row = csv.reader([line]).next()

        if row[0] == " Name":
            return
        # row[1]是职业，float(row[5][1:]是年薪，1是出现次数
        yield row[1],(int(float(row[5][1:])))

    def combiner1(self, key, values):
        '''
        key是职业,values是所有该职业的薪水
        values是元组的列表，元组中第一项为年薪，第二项为出现的次数
        '''
        l = list(values)
        print(key,l)
        # s代表年薪
        s = 0
        # c代表该职业出现的次数
        c = 0
        for i in l:
            s = s + i
            c += 1
        # 【注意】这里的s/c是该node中key职业的平均年薪，c是该node中key职业出现的次数
        yield key, (s/c, c)

    def reducer1(self, key, values):
        '''
        key是职业,values是所有该职业的薪水
        values是元组的列表，元组中第一项为平均年薪，第二项为出现的次数
        '''
        l = list(values)
        print("=====",key,l,"=====")
        # s代表年薪
        s = 0
        # c代表该职业出现的次数
        c = 0
        for i in l:
            s = s+i[0]*i[1]
            c += i[1]

        if c>10:
            yield key,(s/c,c)

        # 只输出样本数大于10的职业
        # if len(l)>10:
        #    yield key,sum(l)/len(l)
        # yield key, sum(l) / len(l)

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
                   combiner = self.combiner1,
                   reducer = self.reducer1),
            MRStep(mapper = self.mapper2,
                   reducer = self.reducer2)
        ]

if __name__ == "__main__":
    AvgSalary.run()

