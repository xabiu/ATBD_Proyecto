from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import desc
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc

spark = SparkSession.builder\
        .master("yarn")\
        .appName("analitica")\
        .getOrCreate()

dataset1 = spark.read.json("datos/yelp_academic_dataset_review.json")
dataset2 = spark.read.json("datos/yelp_academic_dataset_business.json")

top10_reviews = dataset1.groupby('business_id').agg(count("business_id").alias("num_reviews"))
res = top10_reviews.sort(desc("num_reviews")).head(10)
top10_reviews.sort(desc("num_reviews")).show(10)
