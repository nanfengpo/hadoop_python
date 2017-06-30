# coding:utf-8

'''
【级别】
    Median
    
【目的】
    得到最流行的电影的用户评分数以及电影ID。

【知识点】
    如何想到在reducer_count_ratings()中yield None, (sum(values), key)

【执行】
    $ python2 MostPopularMovie.py ./data/ml-100k/u.data 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]
        
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), key)
        
    def reducer_find_max(self, _, values):
        yield max(values)
        
        
if __name__ == '__main__':
    MostPopularMovie.run()