import sys
sys.path.append("./work")

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from utils.schema import sample_schema

def process_stream(stream):
    value_schema = schema_of_json(sample_schema)
    stream = stream \
              .selectExpr("CAST(value AS STRING)") \
              .select(from_json(col("value"), value_schema).alias("value"))
    
    return stream