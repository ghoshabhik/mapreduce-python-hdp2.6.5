from mrjob.job import MRJob
from mrjob.step import MRStep

class MoviePopularity(MRJob):
    def steps(self):
        return[
            MRStep(mapper= self.mapper_get_movie, reducer= self.reducer_get_popularity),
            MRStep(reducer = self.reducer_sort)
        ]

    def mapper_get_movie(self, _, lines):
        (userId, movieId, rating, timestamp) = lines.split(',')
        yield movieId, 1

    def reducer_get_popularity(self, key, values):
        yield str(sum(values)).zfill(5), key

    def reducer_sort(self, count, movies):
        for movie in movies:
            yield movie, count


if __name__ == "__main__":
    MoviePopularity.run()