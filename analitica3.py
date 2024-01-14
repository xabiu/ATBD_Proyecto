from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import desc
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc
from pyspark.sql.functions import split

spark = SparkSession.builder\
        .master("yarn")\
        .appName("analitica")\
        .getOrCreate()

dataset1 = spark.read.json("datos/yelp_academic_dataset_review.json")
dataset2 = spark.read.json("datos/yelp_academic_dataset_business.json")

top10_categorias = dataset2.groupby('city').agg(avg("stars").alias("mean_stars"), count("stars").alias("star_count"))
resultado_filtrado = top10_categorias.filter("star_count >= 10")

res2 = resultado_filtrado.sort(desc("mean_stars")).head(10)
resultado_filtrado.sort(desc("mean_stars")).show(10)

