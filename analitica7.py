from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.functions import desc
from pyspark.sql.functions import avg
from pyspark.sql.functions import asc
from pyspark.sql.functions import split
from pyspark.sql.functions import size
from pyspark.sql.functions import split
from pyspark.sql.functions import col, avg, stddev
from pyspark.sql.functions import sum
from datetime import datetime, timedelta
from pyspark.sql.functions import col, array_contains

spark = SparkSession.builder\
        .master("yarn")\
        .appName("analitica")\
        .getOrCreate()

dataset1 = spark.read.json("datos/yelp_academic_dataset_review.json")
dataset2 = spark.read.json("datos/yelp_academic_dataset_business.json")

top10_categorias = dataset2.groupby(['business_id','categories','stars']).agg(sum("review_count").alias("total_reviews"))
df_split_last = top10_categorias.withColumn("categories_list", split(top10_categorias["categories"], ","))

lista_count = []
lista_categoria = []
for row in df_split_last.rdd.collect():
  if row['categories_list'] is not None:
    for cat in row['categories_list']:
      cat = cat.replace(" ", "", 1)
      if cat not in lista_categoria:
        lista_categoria.append(cat)
        lista_count.append(row['total_reviews'])
      else:
        ind = lista_categoria.index(cat)
        lista_count[ind] += row['total_reviews']

datos_combinados = list(zip(lista_categoria, lista_count))

resultados_ordenados = sorted(datos_combinados, key=lambda x: x[1], reverse=True)[:10]
  #print(resultados_ordenados)

tabla = []
for (cat,num) in resultados_ordenados:
  print("CATEGORIA: ", cat)
  puntuacionesAnuales = []
  datosFiltrados = df_split_last.filter(array_contains(df_split_last['categories_list'], cat))
  datos = datosFiltrados.collect()
  ids = []
  for d in datos:
    ids.append(d['business_id'])
  #print(ids)
  negocio = dataset1.filter(dataset1['business_id'].isin(ids))
  fechaAntigua = negocio.selectExpr("min(date) as FechaMasAntigua").first()["FechaMasAntigua"]
  fechaReciente = negocio.selectExpr("max(date) as FechaMasReciente").first()["FechaMasReciente"]
  fechaAntigua = datetime.strptime(fechaAntigua, "%Y-%m-%d %H:%M:%S")
  fechaReciente = datetime.strptime(fechaReciente, "%Y-%m-%d %H:%M:%S")
  año = 1
  fechaInicio = fechaAntigua
  valor = 0
  while fechaInicio < fechaReciente:
    print(' FECHAS: ' + str(fechaInicio) + '<-->' + str((fechaInicio + timedelta(days=365))))
    entreFechas = negocio.filter((negocio['date'] >= str(fechaInicio)) & (negocio['date'] < str((fechaInicio + timedelta(days=365)))))
    avgStars = entreFechas.groupby('business_id').agg(avg("stars").alias("avg_stars"))
    value = avgStars.collect()
    #print(value)
    if value != []:
      valor = value[0]['avg_stars']
      value = []
    else:
      valor = 0
    print("Media anual: " + str(valor))
    #if len(puntuacionesAnuales) < año:
    puntuacionesAnuales.append([str(fechaInicio) + '<-->' + str((fechaInicio + timedelta(days=365))), valor])
    fila = [cat,str(fechaInicio),str((fechaInicio + timedelta(days=365))),valor]
    tabla.append(fila)
    #else:
      #puntuacionesAnuales[año-1] = valor
    año += 1
    fechaInicio = fechaInicio + timedelta(days=365)
  #print(puntuacionesAnuales)
    #entreFechas.show(5)
    #print(valor)
