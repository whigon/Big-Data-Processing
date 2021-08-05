from mrjob.job import MRJob
from mrjob.job import MRStep
import json
import time


class ScamValueChange(MRJob):
    def mapper_join(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                timestamp = int(fields[6])
                year = time.strftime("%Y", time.gmtime(timestamp))
                mon = time.strftime("%m", time.gmtime(timestamp))
                year_month = str(year) + '-' + str(mon)
                to_address = fields[2]
                value = float(fields[3])

                yield to_address, (0, year_month, value)
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
        year_month = None
        category = None
        value = 0

        for v in values:
            if v[0] == 0:
                year_month = v[1]
                value = v[2]
            else:
                category = v[1]

        if category is not None and year_month is not None:
            yield (year_month, category), value

    def reducer_aggregate(self, key, values):
        yield key, sum(values)

    def steps(self):
        return [MRStep(mapper=self.mapper_join,
                       reducer=self.reducer_join),
                MRStep(reducer=self.reducer_aggregate)]


if __name__ == '__main__':
    ScamValueChange.run()


