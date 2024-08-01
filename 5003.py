from pyspark import SparkContext, SparkConf
import random
import bisect
import findspark

findspark.init()

conf = SparkConf().setAppName("SortRDD")
sc = SparkContext(conf=conf)

n = 100  # size of RDD to be sorted
p = 8    # number of partitions/workers
s = 4    # sample size factor 

r = sc.parallelize([random.randrange(1, n * 100) for _ in range(n)], p)

sample = r.takeSample(False, s * p)

sample_sorted = sorted(sample)
splitters = [sample_sorted[i * s] for i in range(1, p)] 

splitters = [0] + splitters

def split_partition(iterator):
    data = list(iterator)
    result = [[] for _ in range(len(splitters) - 1)]
    for item in data:
        idx = bisect.bisect_right(splitters, item) - 1
        if idx >= 0 and idx < len(result):
            result[idx].append(item)

    for i, bucket in enumerate(result):
        for item in bucket:
            yield (i, item)

split_rdd = r.mapPartitions(split_partition)

grouped_rdd = split_rdd.groupByKey()
sorted_rdd = grouped_rdd.flatMap(lambda x: [(x[0], sorted(x[1]))])

print(sorted_rdd.collect())