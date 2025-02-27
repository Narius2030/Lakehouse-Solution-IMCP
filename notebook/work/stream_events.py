import sys
sys.path.append("./work/imcp")

from utils.config import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import csv_process_batch, csv_process_stream, mobile_process_stream, mobile_process_batch
from utils.schema import *

settings = get_settings()
CSV_DATASET_TOPIC = "minio-parquets"
MOBILE_TOPIC = "mobile-images"

CSV_CHECKPOINT_PATH = "s3a://lakehouse/streaming/imcp/checkpoints/checkpoint_minio"
MOBILE_CHECKPOINT_PATH = "s3a://lakehouse/streaming/imcp/checkpoints/checkpoint_mobile"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Traffic Dataset Spark Streaming")

## TODO: create stream reader
csv_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, CSV_DATASET_TOPIC)
processed_csv_stream = csv_process_stream(csv_stream)

mobile_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, MOBILE_TOPIC)
processed_mobile_stream = mobile_process_stream(mobile_stream)

# TODO: write stream into Delta Lake
write_csv_stream = SparkStreaming.write_microbatch_in_stream(spark, 
                                                             processed_csv_stream, 
                                                             CSV_CHECKPOINT_PATH, 
                                                             settings.MONGODB_ATLAS_URI, 
                                                             csv_process_batch, 
                                                             trigger="60 seconds")
write_csv_stream.start()

write_mobile_stream = SparkStreaming.write_microbatch_in_stream(spark, 
                                                             processed_mobile_stream,
                                                             MOBILE_CHECKPOINT_PATH,
                                                             settings.MONGODB_ATLAS_URI, 
                                                             mobile_process_batch, 
                                                             trigger="10 seconds")
write_mobile_stream.start()




spark.streams.awaitAnyTermination()