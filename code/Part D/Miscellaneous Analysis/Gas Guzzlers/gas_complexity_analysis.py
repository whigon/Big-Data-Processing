from mrjob.job import MRJob

"""
Analysize the contract complexity(block size) and gas used
"""
class GasComplexityAnalysis(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 9:
                size = int(fields[4])
                gas_used = int(fields[6])

                yield size, gas_used
        except:
            pass

    def reducer(self, key, values):
        count = 0
        total = 0

        for v in values:
            count += 1
            total += v

        yield key, total / count


if __name__ == '__main__':
    GasComplexityAnalysis.run()
