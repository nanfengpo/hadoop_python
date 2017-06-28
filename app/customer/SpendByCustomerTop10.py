
# coding:utf-8

'''
【级别】
    median
【目的】
    求出总花费最大的10位顾客
【执行】
    $ python SpendByCustomerSorted.py ./data/customer-orders.csv 

自动根据key排序
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class SpendByCustomerTop10(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_amount,
                   reducer=self.reducer_sum_amount),
            MRStep(mapper=self.mapper_make_amounts_key,
                   reducer=self.reducer_ouput_amounts)
        ]

    def mapper_count_amount(self, _, line):
        (customer, item, orderAmount) = line.split(',')
        yield customer, float(orderAmount)

    def reducer_sum_amount(self, customer, amounts):
        yield customer, sum(amounts)

    def mapper_make_amounts_key(self, customer, amounts):
        yield None,("%04.02f"%float(amounts), customer)

    def reducer_ouput_amounts(self, _, values):
        l = list(values)
        for v in l[-10:]:
            yield v[1],v[0]

if __name__ == '__main__':
    SpendByCustomerTop10.run()
