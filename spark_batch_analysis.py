from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder \
    .appName("SensorBatchAnalysis") \
    .getOrCreate()

# Leer el archivo JSON
df = spark.read.json("datos_sensores.json")

print("=== DATOS CARGADOS (Batch) ===")
df.show()

print("=== PROMEDIOS POR SENSOR (Batch) ===")
batch_result = df.groupBy("sensor_id").agg(
    avg("temperature").alias("avg_temperature"),
    avg("humidity").alias("avg_humidity")
)
batch_result.show()

spark.stop()
