# Oregon State University
# CS 512 - Spark Activities
# Date: 2025/03/07
# Author: Paul J Anderson - Starter code provided by Justin Wolford (Instructor)

# this code is a simple demonstration of how to use the coalesce and repartition functions in PySpark
# the coalesce function is used to reduce the number of partitions in an RDD, while the repartition function
# is used to increase the number of partitions in an RDD. Both functions can be used to optimize the performance

# i've already set up pyspark via SSH, but i'd like to test this code on my local machine with a sample dataset before
# running it on the Google Cloud Platform. i'll use the following code to test the coalesce and repartition functions
# in PySpark on my local machine.
# pip install pyspark in the terminal (admin mode)
# installed Oracle Java 8
# installed Apache Spark 3.2.0
# installed Hadoop 3.3.1
# installed winutils.exe
# set HADOOP_HOME and JAVA_HOME environment variables
# set SPARK_HOME environment variable
# set PATH environment variable
# spark-shell in the terminal

import pyspark
from pyspark import SparkContext, SparkConf
import pprint

# init spark context
sc = SparkContext("local", "Partition Example")

# pretty printer
pp = pprint.PrettyPrinter(indent=4)

# make a list of 25 integers across 3 partitions
rdd = sc.parallelize(range(25), 3)
pp.pprint(rdd.glom().collect())

# make a list of 50 integers across 4 partitions, efficiently convert it to 2 partitions
rdd = sc.parallelize(range(50), 4)
rdd = rdd.coalesce(2)
pp.pprint(rdd.glom().collect())

# starting with a list of 26 integers 0 through 25 on 1 partition, end with a list of 26 integers 
# split among two partitions, even numbers on one and odd on the other
rdd = sc.parallelize(range(26), 1)
even_rdd = rdd.filter(lambda x: x % 2 == 0)
odd_rdd = rdd.filter(lambda x: x % 2 != 0)
combined_rdd = even_rdd.union(odd_rdd).coalesce(2)
pp.pprint(combined_rdd.glom().collect())

# starting with 20 strings split somewhat evenly across 3 partitions, end with 4 partitions 
# with ALL of the strings stored in one with the other 3 empty
strings = ["string" + str(i) for i in range(20)]
rdd = sc.parallelize(strings, 3)
rdd = rdd.coalesce(1).repartition(4)
pp.pprint(rdd.glom().collect())

# compare the results of using repartition(20) directly on an RDD containing the values 0 through 99 
# with the results of first making a key-value pair using the value as the key, then using partitionBy(20)
rdd = sc.parallelize(range(100))
repartitioned_rdd = rdd.repartition(20)
pp.pprint(repartitioned_rdd.glom().collect())

kv_rdd = rdd.map(lambda x: (x, x))
partitioned_rdd = kv_rdd.partitionBy(20)
pp.pprint(partitioned_rdd.glom().collect())

