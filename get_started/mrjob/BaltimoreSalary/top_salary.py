
# coding:utf-8

'''
分别找出AnnualSalary和GrossPay Top10的个人信息
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')


class salarymax(MRJob):
    def mapper(self, _, line):
        # Convert each line into a dictionary
        # 去掉首行
        if csv.reader([line]).next()[0] == " Name":
            return
        # print csv.reader([line]).next()[0]
        row = dict(zip(cols, [a.strip() for a in csv.reader([line]).next()]))

        # Yield the salary
        yield 'salary', (float(row['AnnualSalary'][1:]), line)

        # Yield the gross pay
        try:
            yield 'gross', (float(row['GrossPay'][1:]), line)
        except ValueError:
            # 统计没有GrossPay这一项的个数
            self.increment_counter('warn', 'missing gross', 1)

    def reducer(self, key, values):
        topten = []

        # For 'salary' and 'gross' compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]

        for p in topten:
            yield key, p

    combiner = reducer


if __name__ == '__main__':
    salarymax.run()