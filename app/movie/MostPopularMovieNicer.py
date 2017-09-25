
# coding:utf-8

'''
【级别】
    Hard
    
【目的】
    得到最流行的电影。返回用户评分数以及电影名称。

【改进点】
    相较于MostPopularMovieNicer.py，返回的不是电影ID，而是电影名称，因此要用到reducer_init和add_file_option

【难点】
    - 如何想到在reducer_count_ratings()中yield None, (sum(values), self.movieNames[key])
    - u.data是utf-8格式，u.item是utf-16格式

【执行】
    $ python2 MostPopularMovieNicer.py --items ./data/ml-100k/u.item ./data/ml-100k/u.data 
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import codecs

class MostPopularMovie(MRJob):
    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        self.add_file_option('--item', help='Path to u.item')
    
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.name_dictory,
                   reducer=self.reducer_count_ratings),
            MRStep(reducer=self.reducer_find_max)
        ]
        
    def mapper_get_ratings(self, _, line):
        # 默认用utf-8编码打开文件，而u.data正是utf-8编码，这样用utf8编码将其正确打开
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1
        
    def name_dictory(self):
        self.movieNames = {} # dic = {movieID, movieName}

        # 默认用utf-8编码打开文件，而u.item是utf16编码，这样用utf8编码将其打开会出错，
        # 因此需要使用codecs.open将其以utf-16编码打开。这样的话u.data和u.item的内容就可以相等
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
