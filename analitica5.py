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

numCategorias = dataset2.groupby(['stars', 'categories']).agg(count('stars').alias("numCategories"))

df_split_star = numCategorias.withColumn("categories_list", split(numCategorias["categories"], ","))

lista_count = []
lista_categoria_punt = []
for row in df_split_star.rdd.collect():
  if row['categories_list'] is not None:
    for cat in row['categories_list']:
      if (cat,row['stars']) not in lista_categoria_punt:
        lista_categoria_punt.append((cat,row['stars']))
        lista_count.append(row['numCategories'])
      else:
        ind = lista_categoria_punt.index((cat,row['stars']))
        lista_count[ind] += row['numCategories']

p = 1
tabla = []
for pu in range(1,10):
  datos_combinados = list(zip(lista_categoria_punt, lista_count))
  datos_filtrados = []
  for ((cat,punt),cant) in datos_combinados:
    if punt == p:
      datos_filtrados.append(((cat,punt),cant))
  #categorias_filtradas, reviews_filtradas = zip(*resultados_filtrados)

  resultados_ordenados = sorted(datos_filtrados, key=lambda x: x[1], reverse=True)[:10]

  #tiendas_ordenadas, reviews_ordenadas = zip(*resultados_ordenados)

  # Imprime los resultados
  print("TIENDAS CON PUNTUACIÓN", p, "ORDENADAS POR CANTIDAD DE REVIEWS:")
  for cat, count in resultados_ordenados:
    print(f"{cat[0]} - Puntuación: {cat[1]}, Cantidad de reviews: {count}")
    tabla.append([cat[0], cat[1], count])
  p += 0.5
  print("-----------------------------------------------------------------")
