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

top10_categorias = dataset2.groupby('categories').agg(avg("stars").alias("mean_stars"))
df_split = top10_categorias.withColumn("categories_list", split(top10_categorias["categories"], ","))

lista_categorias = []
lista_puntuaciones = []
lista_apariciones = []
for row in df_split.rdd.collect():
  if row['categories_list'] is not None:
    for cat in row['categories_list']:
      if cat not in lista_categorias:
        lista_categorias.append(cat)
        lista_puntuaciones.append(row['mean_stars'])
        lista_apariciones.append(1)
      else:
        ind = lista_categorias.index(cat)
        lista_puntuaciones[ind] = (lista_puntuaciones[ind] + row['mean_stars'])/2
        lista_apariciones[ind] += 1

ind = 0
borrados = 0
for l in lista_apariciones:
  if l < 100:
    del lista_puntuaciones[ind-borrados]
    del lista_categorias[ind-borrados]
    borrados += 1
  ind += 1

tiendas_con_puntuaciones = list(zip(lista_categorias, lista_puntuaciones))

tiendas_con_puntuaciones.sort(key=lambda x: x[1], reverse=True)

top_10_categorias = tiendas_con_puntuaciones[:10]

print("Las 10 categorías con mayor puntuación media son:")
tabla = []
for categoria, puntuacion in top_10_categorias:
    print(f"{categoria}: {puntuacion}")
    tabla.append([categoria, puntuacion])
