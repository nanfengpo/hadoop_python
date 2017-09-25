
# coding:utf-8
'''
求出平均数量最大的10个年龄
'''

from mrjob.job import MRJob
from mrjob.job import MRStep

class Friend(MRJob):
    # 第一个step，得到每一个年龄的平均朋友数量
    def mapper1(self,_,line):
        row = line.split(',')
        yield row[2],int(row[3])

    def reducer1(self,age,num):
        l = list(num)
        yield age,sum(l)/len(l)

    # 第二个step：求出平均数量最大的10个年龄
    def mapper2(self,age,avernum):
        # 【很重要】mapper2的输入是reducer1的输出
        # 【重要】mapper2执行了几次？有多少个age，就执行了几次！或者说，reducer1输出了几次，mapper2就执行几次
        # 必须把平均朋友数量和相对应的年龄写在同一个元组当中，这样才能一一对应
        yield None,(avernum,age)

    def reducer2(self,_,values):
        # values是元组的列表，列表的每个元素都是平均朋友数量和相对应的年龄组成的一个元组
        # reducer2只执行了一次，因为mapper2中只输出了一个key，那就是None
        l = list(values)
        # l.sort()
        for i in l[-10:]:
            yield i[1],i[0]

    def steps(self):
        return [
            MRStep(mapper = self.mapper1,
                   reducer = self.reducer1
                   ),
            MRStep(mapper = self.mapper2,
                   reducer = self.reducer2
                   )
        ]


if __name__ == '__main__':
    Friend.run()
