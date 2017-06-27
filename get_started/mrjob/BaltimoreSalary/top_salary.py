
# coding:utf-8

'''
分别找出AnnualSalary和GrossPay Top10的个人信息
'''

from mrjob.job import MRJob
# from mrjob.step import MRStep
import csv

cols = 'Name,JobTitle,AgencyID,Agency,HireDate,AnnualSalary,GrossPay'.split(',')


class salarymax(MRJob):
    def mapper(self, _, line):
        # 【注意】line就是CSV文件的每一行，为字符串类型。但要使用csv模块的csv.reader()函数再读一遍才能正确地分隔
        # csv.reader([line]).next()就是把line分隔后的字符串的列表。注意next()不能少,line的中括号也不能少

        # print(type(line)) # 字符串类型
        # print csv.reader([line]).next() # 字符串的列表

        # 使用csv.reader()读取每一行
        row = csv.reader([line]).next()

        # 去掉表头
        if row[0] == " Name":
            return

        # Convert each line into a dictionary
        dic = dict(zip(cols, [a.strip() for a in row]))

        # Yield the salary
        # print("row['AnnualSalary']",row['AnnualSalary'])
        # 下面一行中“[1:]”的目的是去掉开头的“$”字符
        yield 'salary', (float(dic['AnnualSalary'][1:]), line)

        # Yield the gross pay
        try:
            yield 'gross', (float(dic['GrossPay'][1:]), line)
        except ValueError:
            # 统计没有GrossPay这一项的个数
            self.increment_counter('warn', 'missing gross', 1)

        '''
        # 不能使用line.split(',')来处理csv文件！因为不是每个逗号都起到分隔作用！！！
        row = line.split(',')
        if row[0] == ' Name':
            return
        dic = dict(zip(cols,[i.strip() for i in row]))
        print(dic)
        yield 'salary', (float(dic['AnnualSalary'][1:]),line)
        try:
            yield 'gross',(float(dic['GrossPay'][1:]),line)
        except:
            self.increment_counter('warn','missing gross',1)
        '''


    def reducer(self, key, values):
        # 这里的key有两个：'salary'和'gross'
        # 也就是说，最后的reducer会被一个commbiner执行两次
        topten = []

        # For 'salary' and 'gross' compute the top 10
        for p in values:
            topten.append(p)
            topten.sort()
            topten = topten[-10:]

        # 输出'salary'和'gross'的前十名
        for p in topten:
            yield key, p

    combiner = reducer


if __name__ == '__main__':
    salarymax.run()