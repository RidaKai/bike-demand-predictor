from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder \
    .appName("KafkaToHDFS") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("instant", IntegerType(), True),
    StructField("dteday", StringType(), True),
    StructField("season", IntegerType(), True),
    StructField("yr", IntegerType(), True),
    StructField("mnth", IntegerType(), True),
    StructField("hr", IntegerType(), True),
    StructField("holiday", IntegerType(), True),
    StructField("weekday", IntegerType(), True),
    StructField("workingday", IntegerType(), True),
    StructField("weathersit", IntegerType(), True),
    StructField("temp", FloatType(), True),
    StructField("atemp", FloatType(), True),
    StructField("hum", FloatType(), True),
    StructField("windspeed", FloatType(), True),
    StructField("casual", IntegerType(), True),
    StructField("registered", IntegerType(), True),
    StructField("cnt", IntegerType(), True)
])

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "bike-demand-events") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

query = df_parsed.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/data/bike/raw") \
    .option("checkpointLocation", "hdfs://namenode:9000/checkpoints/bike_archive") \
    .outputMode("append") \
    .start()

query.awaitTermination()