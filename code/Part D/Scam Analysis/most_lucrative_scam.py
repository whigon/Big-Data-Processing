from mrjob.job import MRJob
from mrjob.job import MRStep
import json

class MostLucrativeScam(MRJob):
    def mapper_join(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                to_address = fields[2]
                value = float(fields[3])

                yield to_address, (0, value)
            else:
                line = json.loads(lines)
                keys = line["result"]

                for key in keys:
                    record = line["result"][key]
                    category = record["category"]
                    addresses = record["addresses"]

                    for addr in addresses:
                        yield addr, (1, category)
        except:
            pass

    def reducer_join(self, key, values):
        total_value = 0
        category = None
        
        for v in values:
            if v[0] == 0:
                total_value += v[1]
            else:
                category = v[1]
        
        if category is not None:
            yield category, total_value

    def reducer_aggregate(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [MRStep(mapper=self.mapper_join,
                       reducer=self.reducer_join),
                MRStep(reducer=self.reducer_aggregate)]


if __name__ == '__main__':
    MostLucrativeScam.run()
