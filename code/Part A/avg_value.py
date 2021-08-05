from mrjob.job import MRJob
import time


class CountAvgValueOfTransactions(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                timestamp = int(fields[6])
                mon = time.strftime("%m", time.gmtime(timestamp))
                year = time.strftime("%Y", time.gmtime(timestamp))
                key = str(year) + '-' + str(mon)
                value = int(fields[3])

                yield key, value
        except:
            pass

    def reducer(self, key, values):
        sum = 0
        count = 0

        for v in values:
            sum += v
            count += 1
        yield key, sum/count


if __name__ == '__main__':
    CountAvgValueOfTransactions.run()
