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

dataset1 = dataset1.withColumn("numText", size(split(dataset1['text'], " ")))
media_palabras = dataset1.groupBy('stars').agg(avg('numText').alias('mean_words'), stddev('numText').alias('std_words'))

res3 = media_palabras.sort(asc("stars")).head(12)
media_palabras.sort(asc("stars")).show(12)
