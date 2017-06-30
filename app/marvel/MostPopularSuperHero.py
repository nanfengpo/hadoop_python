
# coding:utf-8

'''
【级别】
    Hard
    
【目的】
    得到最流行的英雄名字以及该英雄的朋友个数。流行程度的依据是该英雄与其他英雄的关联程度。

【知识点】
    - 在configure_options(self)函数中调用self.add_file_option()函数，添加文件路径
    - 使用mapper_init。注意mapper_init, reducer_init不能搞混：一个是给mapper task初始化，一个是给reducer task初始化

【注意】
    - Marvel-Graph.txt文件的每一行是每部电影中出场的英雄ID
    - Marvel-Names.txt文件是英雄ID与英雄名的对应表
    - 如何想到在mapper_prep_for_sort()中yield None, (friendsCount, heroName)

【执行】
    $ python2 MostPopularSuperHero.py --names ./data/Marvel-Names.txt ./data/Marvel-Graph.txt 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularSuperHero(MRJob):
    
    def configure_options(self):
        super(MostPopularSuperHero, self).configure_options()
        # add_file_option的作用是为了下面能直接open("Marvel-Names.txt")打开文件
        self.add_file_option('--names', help='Path to Marvel-Names.txt')

    # 第1个step：得到每个英雄的朋友总个数
    def mapper_count_friends_per_line(self, _, line):
        '''
        返回每个英雄（heroID）的朋友个数（numFriends）
        :param _: 
        :param line: 
        :return: int(heroID), int(numFriends)
        '''
        fields = line.split()
        heroID = fields[0]
        numFriends = len(fields) - 1
        yield int(heroID), int(numFriends)
        
    def reducer_combine_friends(self, heroID, friendsCount):
        yield heroID, sum(friendsCount)

    # 第2个step：得到最流行（朋友数最多）的英雄的名字
    def load_name_dictionary(self):
        self.heroName = {}

        with open("Marvel-Names.txt") as f:
            for line in f:
                fields = line.split()
                heroID = int(fields[0])
                self.heroName[heroID] = fields[1]

    def mapper_prep_for_sort(self, heroID, friendsCount):
        heroName = self.heroName[heroID]
        yield None, (friendsCount, heroName)
    
    def reducer_find_max_friends(self, key, value):
        yield max(value)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer=self.reducer_combine_friends),
            MRStep(mapper=self.mapper_prep_for_sort,
                   mapper_init=self.load_name_dictionary,
                   reducer=self.reducer_find_max_friends)
        ]


if __name__ == '__main__':
    MostPopularSuperHero.run()