
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisHomicidiosTransito") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leer el archivo desde Hadoop HDFS
ruta = "hdfs://localhost:9000/Tarea3Batch/homicidios.csv"

df = spark.read.csv(
    ruta,
    header=True,
    inferSchema=True,
    encoding="UTF-8"
)

# 3. Mostrar la estructura y los primeros registros
print("=== ESTRUCTURA DEL DATASET ===")
df.printSchema()

print("=== PRIMEROS REGISTROS ===")
df.show(10, truncate=False)

# 4. Normalizar los nombres de las columnas
# Se supone que el CSV conserva las 9 columnas
# descritas en la fuente, en el mismo orden.
df = df.toDF(
    "departamento",
    "municipio",
    "codigo_dane",
    "armas_medios",
    "fecha_hecho",
    "genero",
    "grupo_etario",
    "conducta",
    "cantidad"
)

# 5. Limpiar registros sin información geográfica
df_limpio = df.filter(
    F.col("departamento").isNotNull() &
    F.col("municipio").isNotNull()
)

df_limpio = df_limpio.withColumn(
    "cantidad",
    F.regexp_replace(
        F.col("cantidad").cast("string"), ",", "."
    ).cast("double")
)

# 6. Mostrar cantidad de registros
print("=== TOTAL DE REGISTROS ORIGINALES ===")
print(df.count())

print("=== REGISTROS DESPUÉS DE LA LIMPIEZA ===")
print(df_limpio.count())

# 7. Estadísticas generales
print("=== RESUMEN ESTADÍSTICO ===")
df_limpio.describe().show()

# 8. Casos agrupados por departamento
print("=== DEPARTAMENTOS CON MÁS CASOS ===")
df_limpio.groupBy("departamento") \
    .agg(
        F.sum("cantidad").alias("total_casos"),
        F.count("*").alias("registros")
    ) \
    .orderBy(F.col("total_casos").desc_nulls_last()) \
    .show(10, truncate=False)

# 9. Casos agrupados por género
print("=== DISTRIBUCIÓN POR GÉNERO ===")
df_limpio.groupBy("genero") \
    .agg(
        F.sum("cantidad").alias("total_casos"),
        F.count("*").alias("registros")
    ) \
    .orderBy(F.col("total_casos").desc_nulls_last()) \
    .show(truncate=False)

# 10. Municipios con más registros
print("=== MUNICIPIOS CON MÁS REGISTROS ===")
df_limpio.groupBy("departamento", "municipio") \
    .count() \
    .orderBy(F.col("count").desc()) \
    .show(10, truncate=False)

spark.stop()
