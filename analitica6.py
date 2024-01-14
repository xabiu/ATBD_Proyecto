from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import desc
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc
from pyspark.sql.functions import split
from pyspark.sql.functions import size
from pyspark.sql.functions import split
from pyspark.sql.functions import col, avg, stddev

spark = SparkSession.builder\
        .master("yarn")\
        .appName("analitica")\
        .getOrCreate()

dataset1 = spark.read.json("datos/yelp_academic_dataset_review.json")
dataset2 = spark.read.json("datos/yelp_academic_dataset_business.json")

cool = dataset1.groupby('cool').agg(avg('stars').alias("avgStars"))
funny = dataset1.groupby('funny').agg(avg('stars').alias("avgStars"))
useful = dataset1.groupby('useful').agg(avg('stars').alias("avgStars"))

tabla = []

#cool
numcool = int(cool.count()/10)-1
f_cool = cool.sort(asc("cool")).collect()
k = numcool-1
for c in f_cool:
  print("cool: " + str(c[0]) + ", Puntuación media: " + str(c[1]))
  if k == numcool:
    tabla.append(['cool', c[0], c[1]])
    k = 0
  else:
    k += 1

#funny
numfunny = int(funny.count()/10)-1
f_funny = funny.sort(asc("funny")).collect()
k = numfunny-1
for c in f_funny:
  if k == numfunny:
    print("funny: " + str(c[0]) + ", Puntuación media: " + str(c[1]))
    tabla.append(['funny', c[0], c[1]])
    k = 0
  else:
    k += 1

#useful
numuseful = int(useful.count()/10)-1
f_useful = useful.sort(asc("useful")).collect()
k = numuseful-1
for c in f_useful:
  if k == numuseful:
    print("useful: " + str(c[0]) + ", Puntuación media: " + str(c[1]))
    tabla.append(['useful', c[0], c[1]])
    k = 0
  else:
    k += 1
