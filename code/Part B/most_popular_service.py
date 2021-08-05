from mrjob.job import MRJob
from mrjob.step import MRStep


class TopTenService(MRJob):
    def mapper_filter(self, _, line):
        try:
            if len(line.split(",")) == 7:
                fields = line.split(",")
                to_address = fields[2]
                value = int(fields[3])

                yield to_address, ("value", value)

            elif len(line.split(",")) == 5:
                fields = line.split(",")
                address = fields[0]

                yield address, "contract"
        except:
            pass

    def reducer_filter(self, key, values):
        bool = False
        vals = []

        for value in values:
            if value[0] == "value":
                vals.append(value[1])
            elif value == "contract":
                bool = True

        if bool:
            yield key, sum(vals)

    def mapper_sort(self, key, value):
        yield None, (key, value)

    def reducer_sort(self, _, key_pair):
        sorted_values = sorted(key_pair, reverse=True, key=lambda x: x[1])

        for value in sorted_values[:10]:
            yield value[0], value[1]

    def steps(self):
        return [MRStep(mapper=self.mapper_filter,
                       reducer=self.reducer_filter),
                MRStep(mapper=self.mapper_sort,
                       reducer=self.reducer_sort)]


if __name__ == '__main__':
    TopTenService.run()

"""
python most_popular_service.py -r hadoop --output-dir out --no-cat-output 
hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/transactions/* 
hdfs://andromeda.eecs.qmul.ac.uk/data/ethereum/contracts/* 
"""