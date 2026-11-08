from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

spark = SparkSession.builder \
    .appName("BatchKPIs") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_bike = spark.read.parquet("hdfs://namenode:9000/data/bike/raw")

print("Indicateurs de la demande de vélos")

print("\nLocations par heure de la journée :")
df_bike.groupBy("hr").agg(
    avg("cnt").alias("moyenne_locations"),
    count("*").alias("nombre_enregistrements")
).orderBy("hr").show()

print("\nLocations moyennes par condition météo :")
df_bike.groupBy("weathersit").agg(
    avg("cnt").alias("moyenne_locations")
).orderBy("weathersit").show()

print("\nLocations moyennes par jour de la semaine :")
df_bike.groupBy("weekday").agg(
    avg("cnt").alias("moyenne_locations")
).orderBy("weekday").show()

print("\nTerminé !")