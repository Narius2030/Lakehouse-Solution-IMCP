import sys
sys.path.append("./work/imcp")

import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime
from utils.schema import csv_sample_schema
from utils.udf_helpers import tokenize_vietnamese

def process_stream(stream):
    value_schema = F.schema_of_json(csv_sample_schema)
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), value_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def clean_caption(df_file, column):
    regex_pattern = r'[!“"”#$%&()*+,./:;<=>?@\[\\\]\^{|}~-]'
    df_cleaned = (df_file.withColumn(column, F.regexp_replace(F.col(column), regex_pattern, ""))
                        .withColumn(column, F.lower(F.col(column)))
                        .withColumn(f"{column}_tokens", tokenize_vietnamese(F.col("caption")))
                        .withColumn("word_count", F.size(f"{column}_tokens"))
                        .withColumn("created_time", F.lit(datetime.now()))
                 )
    return df_cleaned

def process_batch(df, batch_id, spark, db_uri):
    for row in df.collect():
        file_path = f"s3a://{row['Key']}"
        df_file = (spark.read
                    .csv(file_path, header=True)
                    .dropDuplicates()
                  )
        df_cleaned = clean_caption(df_file, "caption")
        
        (df_cleaned.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", db_uri)
                .option("spark.mongodb.write.database", "imcp")
                .option("spark.mongodb.write.collection", "raw")
                .option("spark.mongodb.write.batch.size", "10000")
                .mode("append")
                .save()
        )