import pyspark
from operator import add
from time import *


def is_good_transaction_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 7:
            return False
        int(fields[3])
        return True
    except:
        return False


def is_good_contract_line(line):
    try:
        fields = line.split(',')
        if len(fields) != 5:
            return False
    except:
        return False
    return True


def remove_none(features):
    try:
        if features[1][1] is None:
            return False
    except:
        return False
    return True


begin_time = time()

sc = pyspark.SparkContext()

transactions = sc.textFile("/data/ethereum/transactions/")
contracts = sc.textFile("/data/ethereum/contracts/")

# Filter and extract values
transaction_features = transactions.filter(is_good_transaction_line).map(lambda l: (l.split(",")[2], int(l.split(",")[3])))
# Aggregation
transaction_values = transaction_features.reduceByKey(add)

# Filter and extract address
contract_address = contracts.filter(is_good_contract_line).map(lambda l: (l.split(",")[0], "contract"))

# Join values
join_features = contract_address.leftOuterJoin(transaction_values)
# Remove None value
filter_features = join_features.filter(remove_none)
# Remove the label info
result = filter_features.map(lambda l: (l[0], l[1][1]))

# Sort
top10 = result.takeOrdered(10, key=lambda x: -x[1])
for record in top10:
    print("{}: {}".format(record[0], record[1]))

# Save to file
sc.parallelize(top10).saveAsTextFile("top-10")

end_time = time()
print("time: " + str(end_time - begin_time))
