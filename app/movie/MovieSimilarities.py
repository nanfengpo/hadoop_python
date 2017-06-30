
# coding:utf-8

'''
【级别】
    Hard
    
【目的】
    得到所有关联程度大于0.95的两个电影。

【执行】

# To run locally:
# python MovieSimilarities.py --items ./data/ml-100k/u.item  ./data/ml-100k/u.data > sims.txt
# 需要运行8分钟

# To run on hadoop cluster:
# python MovieSimilarities.py -r hadoop --items ./data/ml-100k/u.item  ./data/ml-100k/u.data > sims.txt
# 三台机器集群时需要运行5分钟

【结果】
保存在sims.txt。例如第132453行，与星球大战（1977）相似度最高的两部电影分别是星球大战（1980）、星球大战（1983）
"Star Wars (1977)"	["Return of the Jedi (1983)", 0.9857230861253026, 480]
"Star Wars (1977)"	["Empire Strikes Back, The (1980)", 0.9895522078385338, 345]

'''

from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

import codecs
import time


class MovieSimilarities(MRJob):

    def configure_options(self):
        super(MovieSimilarities, self).configure_options()
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]

    #第1个step：得到每个用户的所有评分信息(电影ID,评分)
    def mapper_parse_input(self, key, line):
        # 得到u.data中每一行的打分情况
        # Outputs: userID => (movieID, rating)
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield  userID, (movieID, float(rating))

    def reducer_ratings_by_user(self, user_id, itemRatings):
        #Group (item, rating) pairs by userID
        # 把同一个用户的所有评分(电影ID,评分)放在一起，形成一个列表ratings
        # ratings是元组的列表

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))

        yield user_id, ratings

    # 第2个step：得到每个用户的所有评分信息(电影ID,评分)
    def mapper_create_item_pairs(self, user_id, itemRatings):
        # Find every pair of movies each user has seen, and emit
        # each pair with its associated ratings

        # "combinations" finds every possible pair from the list of movies
        # this user viewed.
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            # Produce both orders so sims are bi-directional
            yield (movieID1, movieID2), (rating1, rating2)
            yield (movieID2, movieID1), (rating2, rating1)


    def cosine_similarity(self, ratingPairs):
        # Computes the cosine similarity metric between two
        # rating vectors.
        # 返回两个电影的相似度（score）和有多少对rating（也就是有多少用户同时为这两个电影打分）
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        # Compute the similarity score between the ratings vectors
        # for each movie pair viewed by multiple people

        # Output movie pair => score, number of co-ratings

        score, numPairs = self.cosine_similarity(ratingPairs)

        # Enforce a minimum score and minimum number of co-ratings
        # to ensure quality
        # 只保留相似度大于0.95且超过10人同时打分的电影对
        if (numPairs > 10 and score > 0.95):
            yield moviePair, (score, numPairs)

    def mapper_sort_similarities(self, moviePair, scores):
        # Shuffle things around so the key is (movie1, score)
        # so we have meaningfully sorted results.
        score, n = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[int(movie1)], score), \
            (self.movieNames[int(movie2)], n)

    def load_movie_names(self):
        # Load database of movie names.
        self.movieNames = {}

        with codecs.open("u.item",encoding='utf16') as f:
            for line in f:
                fields = line.split('|')
                self.movieNames[int(fields[0])] = fields[1]

    def reducer_output_similarities(self, movieScore, similarN):
        # Output the results.
        # Movie => Similar Movie, score, number of co-ratings
        movie1, score = movieScore
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)


if __name__ == '__main__':
    time1 = time.time()
    MovieSimilarities.run()
    time2 = time.time()
    # print time2-time1
