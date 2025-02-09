import sys
sys.path.append("./work/imcp")

import pyspark.sql.functions as F
import pyspark.sql.types as T
from utils.schema import csv_sample_schema

def process_stream(stream):
    value_schema = F.schema_of_json(csv_sample_schema)
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), value_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def read_stopwords():
    # Đọc stopwords từ file
    with open("./data/stopwords.txt", "r", encoding="utf-8") as file:
        custom_stopwords = list(line.strip() for line in file)
    return custom_stopwords

def remove_stopwords(words):
    stopwords = read_stopwords()
    words = [word for word in words if word not in stopwords]
    return words

def normalize_caption(df_file, column):
    df_file = df_file.drop("short_caption").dropDuplicates()  # Xử lý làm sạch data
    df_cleaned = (
                    df_file.withColumn(column, F.regexp_replace(F.col(column), "[^a-zA-Z0-9\\s]", ""))
                        .withColumn(f"{column}_tokens", F.split(F.col(column), " "))
                 )
    
    udf_remove_stopwords = F.udf(remove_stopwords, T.ArrayType(T.StringType()))
    df_cleaned = (
                    df_cleaned.withColumn("word_count", F.count(F.size(f"{column}_tokens")))
                            .withColumn("short_caption_tokens", udf_remove_stopwords(F.col(f"{column}_tokens")))
                 )
    
    df_cleaned = df_cleaned.limit(10)
    return df_cleaned

def process_batch(df, batch_id, spark, settings):
    for row in df.collect():
        file_path = f"s3a://{row['Key']}"
        df_file = (spark.read
                    .format("parquet")
                    .option("header", "true")
                    .load(file_path)
                  )
        df_cleaned = normalize_caption(df_file, "caption")
        
        (
            df_cleaned.write
                    .format("mongodb") \
                    .option("spark.mongodb.write.connection.uri", settings.MONGODB_ATLAS_URI) \
                    .option("spark.mongodb.write.database", "imcp") \
                    .option("spark.mongodb.write.collection", "audit") \
                    .option("spark.mongodb.write.batch.size", "10000") \
                    .mode("append") \
                    .save()
        )