from mrjob.job import MRJob
from mrjob.step import MRStep


class TopTenMiner(MRJob):
    def mapper_aggregate(self, _, line):
        try:
            fields = line.split(",")
            if len(fields) == 9:
                miner = fields[2]
                size = int(fields[4])
                yield miner, size
        except:
            pass

    def reducer_aggregate(self, key, values):
        yield None, (key, sum(values))

    def reducer_sort(self, _, values):
        sorted_values = sorted(values, reverse=True, key=lambda x: x[1])

        for v in sorted_values[:10]:
            yield v[0], v[1]

    def steps(self):
        return [MRStep(mapper=self.mapper_aggregate,
                       reducer=self.reducer_aggregate),
                MRStep(reducer=self.reducer_sort)]


if __name__ == '__main__':
    TopTenMiner.run()