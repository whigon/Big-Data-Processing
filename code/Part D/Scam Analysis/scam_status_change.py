from mrjob.job import MRJob
import json
import time


class ScamChange(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                timestamp = int(fields[6])
                year = time.strftime("%Y", time.gmtime(timestamp))
                mon = time.strftime("%m", time.gmtime(timestamp))
                year_month = str(year) + '-' + str(mon)
                to_address = fields[2]

                yield to_address, (0, year_month)
            else:
                line = json.loads(lines)
                keys = line["result"]

                for key in keys:
                    record = line["result"][key]
                    category = record["category"]
                    addresses = record["addresses"]
                    status = record["status"]

                    for addr in addresses:
                        yield addr, (1, category, status)
        except:
            pass

    def reducer(self, key, values):
        year_month = None
        category = None
        status = None

        for v in values:
            if v[0] == 0:
                year_month = v[1]
            else:
                category = v[1]
                status = v[2]

        if year_month is not None and category is not None:
            yield year_month, (category, status)


if __name__ == '__main__':
    ScamChange.run()
