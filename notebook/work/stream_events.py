import sys
sys.path.append("./work/imcp")

from utils.config import get_settings
from operators.streaming import SparkStreaming
from utils.streaming_functions import process_batch, process_stream
from utils.schema import *

settings = get_settings()
CSV_DATASET_TOPIC = "minio-parquets"
CSV_CHECKPOINT_PATH = "s3a://lakehouse/streaming/imcp/checkpoints/checkpoint_minio"

## TODO: create a SparkSession
spark = SparkStreaming.get_instance(app_name="Traffic Dataset Spark Streaming")

## TODO: create stream reader
csv_stream = SparkStreaming.create_kafka_read_stream(spark, settings.KAFKA_ADDRESS, settings.KAFKA_PORT, CSV_DATASET_TOPIC)
processed_csv_stream = process_stream(csv_stream)

## TODO: write stream into Delta Lake
write_csv_stream = SparkStreaming.write_microbatch_in_stream(spark, 
                                                             processed_csv_stream, 
                                                             CSV_CHECKPOINT_PATH, 
                                                             settings.MONGODB_ATLAS_URI, 
                                                             process_batch, 
                                                             write_format="console")
write_csv_stream.start()

spark.streams.awaitAnyTermination()