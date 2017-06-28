
# coding:utf-8
'''
【级别】
    simple
【目的】
    求出每位消费者的所有支出
【执行】
    $ python SpendByCustomer.py ./data/customer-orders.csv 
'''


from mrjob.job import MRJob

class SpendByCustomer(MRJob):

    def mapper(self, _, line):
        # 因为是csv文件，且所有逗号都是分隔符，这里直接使用.split(',')
        (customer, item, orderAmount) = line.split(',')
        yield customer, float(orderAmount)

    def reducer(self, customer, orders):
        yield customer, sum(orders)

if __name__ == '__main__':
    SpendByCustomer.run()
