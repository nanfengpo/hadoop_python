
# coding:utf-8

'''
原始代码
找出平均年薪（AnnualSalary） Top10的行业JobTitle
相比较与avg_salary.py的改进：
增加了combiner：创建了新的avgcombiner()。且去除了c<3时丢弃数据的情况，只在avgreducer中丢弃数据
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')

class salaryavg(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.avgmapper,
                   combiner=self.avgcombiner, # 【注意】这里有无combiner都会得到正确结果
                   reducer=self.avgreducer),
            MRStep(mapper=self.ttmapper,
                   combiner=self.ttreducer,
                   reducer=self.ttreducer) ]

    # 第1步：得到每个行业的平均年薪
    def avgmapper(self, _, line):

        # 转化为字符串的列表
        row = csv.reader([line]).next()

        # Convert each line into a dictionary
        # 去掉首行
        if row[0] == " Name":
            return
        dic = dict(zip(cols, [ a.strip() for a in row]))

        # self.increment_counter('depts', dic['Agency'], 1)

        yield dic['JobTitle'], (int(float(dic['AnnualSalary'][1:])), 1) #这里加上int()的目的，是只保留整数，否则会计算到小数点后8位

    def avgcombiner(self, key, values):
        # key是职业
        # print('type(values):',type(values))
        s = 0
        c = 0

        for average, count in values:
            s += average * count
            c += count
            # 只有在第一个step中加上combiner=self.avgreducer，才会出现count>1的情况（在reducer中）
            # 否则，直接由avgmapper传给reducer，会导致values中每个count都为1
            # if count>1:
                # print("======count>1=====\n",count)

        # 【注意】这里会产生问题！我们知道，一个combiner对应一个mapper，也就是说一个combiner处理一个mapper产生的所有数据
        # 在一个combiner当中，若对应的这个mapper产生的某个职业的样本数小于3个，则直接舍弃，但可能其他mapper中还有该职业的样本！
        # 因此combiner当中不能舍弃！应该将combiner和reducer区分开来！只在reducer中加上判断语句，去除总样本数小于某个值（例如3）的职业
        yield key, (s/c, c)

    def avgreducer(self, key, values):
        # key是职业
        # print('type(values):',type(values))
        s = 0
        c = 0

        for average, count in values:
            s += average * count
            c += count
            # 只有在第一个step中加上combiner=self.avgreducer，才会出现count>1的情况（在reducer中）
            # 否则，直接由avgmapper传给reducer，会导致values中每个count都为1
            # if count>1:
                # print("======count>1=====\n",count)

        # 【注意】这里会产生问题！我们知道，一个combiner对应一个mapper，也就是说一个combiner处理一个mapper产生的所有数据
        # 在一个combiner当中，若对应的这个mapper产生的某个职业的样本数小于3个，则直接舍弃，但可能其他mapper中还有该职业的样本！
        # 因此combiner当中不能舍弃！应该将combiner和reducer区分开来！只在reducer中加上判断语句，去除总样本数小于某个值（例如3）的职业
        if c > 3:
            yield key, (s/c, c)
        else:
            self.increment_counter('stats', 'below3', 1)

    # 第2步：得到平均年薪最高的10个行业
    def ttmapper(self, key, value):
        # 【注意】所有的输出都以None为key，也就是只有一个key！导致的结果就是所有的(value[0], key)会形成一个generator输出
        # 为什么要这么做？
        # 这是因为，第1步的reducer最后的输出结果是每个职业对应的平均年薪。现在要做的是对所有职业按照年薪来排序，
        # 如果不把所有的职业放在一块，就没法对职业排序！因此必须把所有职业和其平均年薪放在一块，用于排序
        yield None, (value[0], (key,value[1])) # group by all, keep average and job title

    def ttreducer(self, key, values):
        #print("values:")
        # 求top10的惯用做法
        topten = []
        for average, job in values:
            topten.append((average, job))
            topten.sort()
            topten = topten[-10:]

        for average, job in topten:
            yield None, (average, job)


if __name__ == '__main__':
    salaryavg.run()