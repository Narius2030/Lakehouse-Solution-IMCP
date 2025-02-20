import sys
sys.path.append("./work/imcp")

import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime
from utils.schema import csv_sample_schema, image_schema
from utils.udf_helpers import tokenize_vietnamese, generate_caption, upload_image

def csv_process_stream(stream):
    value_schema = F.schema_of_json(csv_sample_schema)
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), value_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def mobile_process_stream(stream):
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), image_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def clean_caption(df, column):
    regex_pattern = r'[!“"”#$%&()*+,./:;<=>?@\[\\\]\^{|}~-]'
    df_cleaned = (df.withColumn(column, F.regexp_replace(F.col(column), regex_pattern, ""))
                    .withColumn(column, F.lower(F.col(column)))
                    .withColumn(f"{column}_tokens", tokenize_vietnamese(F.col("caption")))
                    .withColumn("tokenized_caption", F.concat_ws(' ', f"{column}_tokens"))
                    .withColumn("word_count", F.size(f"{column}_tokens"))
                    .withColumn("created_time", F.lit(datetime.now()))
                    .drop("image_base64", "image_name")
                 )
    return df_cleaned

def format_user_data(df):
    formatted_df = (df.withColumn("original_url", F.concat(F.lit("http://160.191.244.13:9000/lakehouse/user-data/images/"), F.col('image_name')))
                    .withColumn("source_website", F.lit("Mobile"))
                    .withColumn("search_query", F.lit("None"))
                    .withColumn("resolution", F.col('image_size'))
                    .withColumn("caption", F.lit(" "))
                    .drop("image_size")
                   )
    return formatted_df


def csv_process_batch(df, batch_id, spark, db_uri):
    for row in df.collect():
        file_path = f"s3a://{row['Key']}"
        df_file = (spark.read
                    .csv(file_path, header=True)
                    .drop("local_path")
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
        
def mobile_process_batch(df, batch_id, spark, db_uri):
    formatted_df = format_user_data(df)
    formatted_df = (formatted_df.withColumn("upload_status", upload_image(F.col("image_base64"), F.col("image_name"))))
    # formatted_df = formatted_df.withColumn("caption", generate_caption(F.col("original_url")))
    formatted_df = clean_caption(formatted_df, 'caption')

    (formatted_df.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", db_uri)
                .option("spark.mongodb.write.database", "imcp")
                .option("spark.mongodb.write.collection", "user_data")
                .option("spark.mongodb.write.batch.size", "10000")
                .mode("append")
                .save()
    )