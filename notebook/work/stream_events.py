import sys
sys.path.append("./work")
from operators.streaming import SparkStreaming
from utils.streaming_functions import process_stream

KAFKA_ADDRESS="160.191.244.13"
KAFKA_PORT="9092"
TOPIC="test"
STORAGE_PATH="s3a://lakehouse/streaming/test/taxi_trends"
CHECKPOINT_PATH = "s3a://lakehouse/streaming/test/checkpoint_taxi"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Spark Streaming")

## TODO: create stream reader
stream = SparkStreaming.create_kafka_read_stream(spark, KAFKA_ADDRESS, KAFKA_PORT, TOPIC)

## TODO: process stream
processed_stream = process_stream(stream)

## TODO: write stream into Delta Lake
write_stream = SparkStreaming.create_file_write_stream(processed_stream, STORAGE_PATH, CHECKPOINT_PATH)
write_stream.start()

spark.streams.awaitAnyTermination()