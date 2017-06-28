
# coding:utf-8

'''
目的：
统计每个年龄的平均朋友个数。例如age年龄有total / numElements个朋友。

执行：
python2 FriendsByAge.py ./data/fakefriends.csv

'''

from mrjob.job import MRJob

class MRFriendsByAge(MRJob):

    def mapper(self, _, line):
        (ID, name, age, numFriends) = line.split(',')
        yield age, int(numFriends) # 这里使用int而不是float，为的是使得结果为整型

    def reducer(self, age, numFriends):
        total = 0
        numElements = 0
        for x in numFriends:
            total += x
            numElements += 1

        yield age, total / numElements

if __name__ == '__main__':
    MRFriendsByAge.run()
