# coding:utf-8

'''
目的：
得到最流行的英雄名字以及该英雄的朋友个数。流行程度的依据是该英雄与其他英雄的关联程度。

注意：
- Marvel-Graph.txt文件的每一行是每部电影中出场的英雄ID
- Marvel-Names.txt文件是英雄ID与英雄名的对应表
- 如何想到在mapper_prep_for_sort()中yield None, (friendsCount, heroName)
- 相较于MostPopularSuperHero.py，MostPopularSuperHero2.py减少了step2的mapper，整合到了step1中的reducer中

执行：
python2 MostPopularSuperHero2.py --names ../data/Marvel-Names.txt <../data/Marvel-Graph.txt 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep


class MostPopularSuperHero(MRJob):
    def configure_options(self):
        super(MostPopularSuperHero, self).configure_options()
        self.add_file_option('--names', help='Path to Marvel-Names.txt')

    def load_name_dictionary(self):
        self.heroName = {}

        with open("Marvel-Names.txt") as f:
            for line in f:
                fields = line.split()
                heroID = int(fields[0])
                self.heroName[heroID] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_count_friends_per_line,
                   reducer_init=self.load_name_dictionary,
                   reducer=self.reducer_combine_friends),
            MRStep(reducer=self.reducer_find_max_friends)
        ]

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
        heroName = self.heroName[heroID]
        yield None, (sum(friendsCount), heroName)

    def reducer_find_max_friends(self, key, value):
        yield max(value)


if __name__ == '__main__':
    MostPopularSuperHero.run()