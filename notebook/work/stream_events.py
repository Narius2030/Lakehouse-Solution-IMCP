import sys
sys.path.append("./work")

from utils.configuration import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import process_stream
from pyspark.sql.functions import col, lit, to_timestamp, split, size

settings = get_settings()
TOPIC="minio-parquets"
CHECKPOINT_PATH = "s3a://lakehouse/streaming/test/checkpoint_minio_parquets"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Spark Streaming")

## TODO: create stream reader
stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, TOPIC)

## TODO: process stream
processed_stream = process_stream(stream)

## TODO: write stream into Delta Lake
write_stream = SparkStreaming.create_file_write_stream(processed_stream, checkpoint_path=CHECKPOINT_PATH, output_mode="append", file_format="console")
write_stream.start()

spark.streams.awaitAnyTermination()


# df = spark.read.format("mongodb") \
#             .option("spark.mongodb.read.connection.uri", settings.MONGODB_ATLAS_URI) \
#             .option("spark.mongodb.read.database", "imcp") \
#             .option("spark.mongodb.read.collection", "raw") \
#             .load()

# df = df.withColumn("created_time", to_timestamp(col("created_time"), "yyyy-MM-dd HH:mm:ss")) \
#         .withColumn("caption_size", size(split(df["caption"], " ")))

# print(df.printSchema())
# print(df.show(5))
# print(df.filter(df["caption_size"] < 100).count())