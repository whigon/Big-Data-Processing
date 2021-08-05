from mrjob.job import MRJob
import time


class GasPriceChange(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 7:
                timestamp = int(fields[6])
                gas_price = int(fields[5])
                year = time.strftime("%Y", time.gmtime(timestamp))
                mon = time.strftime("%m", time.gmtime(timestamp))
                key = str(year) + '-' + str(mon)

                yield key, gas_price
        except:
            pass

    def reducer(self, key, values):
        price = 0
        count = 0
        
        for v in values:
            price += v
            count += 1

        yield key, price/count


if __name__ == '__main__':
    GasPriceChange.run()