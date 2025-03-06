# Oregon State University
# CS 512 - Spark Plane Distances 2
# Date: 2025/03/05
# Author: Paul J Anderson & Rachel Hughes - Starter code provided by Justin Wolford

# Scrub
# this code was used in Google Big Query to filter the data for planes missing data
# CREATE TABLE `cs512-447721.aircraft_data.plane_data_cleaned` 
# AS SELECT * 
# FROM `cs512-447721.aircraft_data.plane_data` 
# WHERE (PosTime IS NOT NULL 
# AND Icao IS NOT NULL 
# AND Lat IS NOT NULL 
# AND Long1 IS NOT NULL 
# AND Alt IS NOT NULL 
# AND Lat != 0.0 
# AND Long1 != 0.0);

import pyspark
from pyspark.sql import SparkSession
import pprint
import json
from pyspark.sql.types import StructType, FloatType, LongType, StringType, StructField
from pyspark.sql import Window
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import lead, udf, struct, col

### haversine distance
def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371 # Radius of earth in kilometers. Use 3956 for miles
    return float(c * r)

# modified to trouble shoot the missing keys
def To_numb(x): # converts the strings to integers and floats
  x['PosTime'] = int(x['PosTime']) # default to 0 if 'PosTime' is not a number
  #x['FSeen'] = int(x['FSeen'])
  x['Lat'] = float(x['Lat']) # default to 0.0 if 'Lat' is not a number
  x['Long1'] = float(x['Long1']) # default to 0.0 if 'Long1' is not a number
  return x

sc = pyspark.SparkContext()

#PACKAGE_EXTENSIONS= ('gs://hadoop-lib/bigquery/bigquery-connector-hadoop2-latest.jar')

bucket = sc._jsc.hadoopConfiguration().get('fs.gs.system.bucket')
project = sc._jsc.hadoopConfiguration().get('fs.gs.project.id')
input_directory = 'gs://{}/hadoop/tmp/bigquerry/pyspark_input'.format(bucket)
output_directory = 'gs://{}/pyspark_demo_output'.format(bucket)

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('flights') \
  .getOrCreate()

conf={
    'mapred.bq.project.id':project,
    'mapred.bq.gcs.bucket':bucket,
    'mapred.bq.temp.gcs.path':input_directory,
    'mapred.bq.input.project.id': "cs512-447721",
    'mapred.bq.input.dataset.id': 'aircraft_data',
    'mapred.bq.input.table.id': 'plane_data_cleaned',
}

## pull table from big query
table_data = sc.newAPIHadoopRDD(
    'com.google.cloud.hadoop.io.bigquery.JsonTextBigQueryInputFormat',
    'org.apache.hadoop.io.LongWritable',
    'com.google.gson.JsonObject',
    conf = conf)

## convert table to a json like object, turn PosTime and Fseen back into numbers... not sure why they changed
vals = table_data.values()
vals = vals.map(lambda line: json.loads(line))
vals = vals.map(To_numb)

##schema -- this is about where we left off in the last assignment --
schema = StructType([
#   StructField('FSeen', LongType(), True), # again, commenting out FSeen
   StructField("Icao", StringType(), True),
   StructField("Lat", FloatType(), True),
   StructField("Long1", FloatType(), True),
   StructField("PosTime", LongType(), True)])

## create a dataframe object
df1 = spark.createDataFrame(vals, schema= schema)

# we break this up into six partition, 6 nodes
df1.repartition(6) 

## create window by partitioning by Icao and ordering by PosTime, then use lead to get next lat long
# calculates the distance between the two points
# window looks at one row at a time, and the next row
# we're partitioning by Icao, and ordering by PosTime -- if not ordered by PosTime, you might jump out of sequence
# is this ever called data crawling or a sliding window? I think it is
window = Window.partitionBy("Icao").orderBy("PosTime").rowsBetween(1,1)
# creates a new colum
df1=df1.withColumn("Lat2", lead('Lat').over(window))
df1=df1.withColumn("Long2", lead('Long1').over(window))
df1 = df1.na.drop()
#pprint.pprint(df1.take(5))
#print(df1.dtypes)

# apply the haversine function to each set of coordinates
# passing in uswer defined function - typcially want to avoid this; it's slow and spark can't optimize it
haver_udf = udf(haversine, FloatType())
df1 = df1.withColumn('dist', haver_udf('long1', 'lat', 'long2', 'lat2'))
#pprint.pprint(df1.take(5))

## sum the distances for each Icao to get distance each plane traveled
df1.createOrReplaceTempView('planes')
top = spark.sql("Select Icao, SUM(dist) as dist FROM planes GROUP BY Icao ORDER BY dist desc LIMIT 10 ")
top = top.rdd.map(tuple)
pprint.pprint(top.collect())

# ### convert the dataframe back to RDD
# we would use this if we wanted to use the RDD API instead of SQL
# we're not using this, but it's here for reference
# dist = df1.rdd.map(list)

# ### apply the haversine equation on each row
# dist = dist.map(lambda x: x+[haversine(x[3],x[2],x[6],x[5])])

# ### create rdd of (Icao, dist)
# dist = dist.map(lambda x: (x[1] , x[7]))

# ### sum each by Icao key, and sort
# dist = dist.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1], ascending = False)
# pprint.pprint(dist.take(10))

# ### collect total of all flights
# total = dist.values().reduce(lambda x,y: x+y)
# print(total)

# Determine if distance traveled is greater than the longest distance flight
# from Singapore to JFK (15349 km)
top = spark.sql("""
    SELECT Icao, SUM(dist) as dist 
    FROM planes 
    GROUP BY Icao 
    HAVING SUM(dist) < 15349 
    ORDER BY dist DESC 
    LIMIT 10
""")
top = top.rdd.map(tuple)
pprint.pprint(top.collect())

# Sum the distances for all planes, selecting only distances less than the maximum distance flight
miles = spark.sql("""
    SELECT SUM(dist) 
    FROM (
        SELECT dist 
        FROM planes 
        WHERE dist < 15349
    ) AS tmp
""")
pprint.pprint(miles.collect())

## deletes the temporary files
input_path = sc._jvm.org.apache.hadoop.fs.Path(input_directory)
input_path.getFileSystem(sc._jsc.hadoopConfiguration()).delete(input_path, True)

