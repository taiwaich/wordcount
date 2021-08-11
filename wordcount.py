from __future__ import print_function
import sys
from operator import add
from pyspark.sql import SparkSession
if __name__ == "__main__":
    filename = sys.argv[1]
    print("filename is ", filename)
    if len(sys.argv) != 2:
        print("Usage: wordcount <file>", file=sys.stderr)
        sys.exit(-1)

    wordsCount = 0
    spark = SparkSession\
         .builder\
         .appName("PythonWordCount")\
         .getOrCreate()

    lines = spark.read.text(sys.argv[1]).rdd.map(lambda r: r[0])
    if filename.endswith('.txt'):
         counts = lines.flatMap(lambda x: x.split(' ')) \
                                  .map(lambda x: (x, 1)) \
                                  .reduceByKey(add).sortBy(lambda a:-a[1])
    elif filename.endswith('.csv'):
         counts = lines.flatMap(lambda x: x.split(',')) \
                                  .map(lambda x: (x, 1)) \
                                  .reduceByKey(add).sortBy(lambda a:-a[1])
    output = counts.take(1000)
    for (word, count) in output:
            wordsCount += 1
            print("%s: %i" % (word, count))
    print("number of words", wordsCount)
    spark.stop()
