
# coding:utf-8

'''
【级别】
    Median
【目的】
    求出每位消费者的所有支出，并排序
【执行】
    $ python SpendByCustomerSorted.py ./data/customer-orders.csv 

自动根据key排序
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class SpendByCustomerSorted(MRJob):

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
        yield "%04.02f"%float(amounts), customer

    def reducer_ouput_amounts(self, amounts, customers):
        for customer in customers:
            yield amounts, customer

if __name__ == '__main__':
    SpendByCustomerSorted.run()
