
# coding:utf-8

'''
目的：
得到最流行的电影。返回用户评分数以及电影ID。

难点：
如何想到在reducer_count_ratings()中yield None, (sum(values), key)

执行：
python2 MostPopularMovieNicer.py --items ../data/ml-100k/u.item <../data/ml-100k/u.data 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import codecs

class MostPopularMovie(MRJob):
    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]
        
    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def reducer_init(self):
        self.movieNames = {} # dic = {movieID, movieName}

        # python2默认用ascii编码打开文件，而u.item是utf16编码，这样用ascii编码将其encode为字符串会出错，
        # 因此需要使用codecs.open将其以utf-16编码打开
        with codecs.open("u.item",encoding='utf16') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[fields[0]] = fields[1]

    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key])
        
    def reducer_find_max(self, key, values):
        yield max(values)
        
        
if __name__ == '__main__':
    MostPopularMovie.run()