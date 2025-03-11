import sys
sys.path.append("./work/imcp")

import pyspark.sql.functions as F                                                       # type: ignore
import pyspark.sql.types as T                                                           # type: ignore
from utils.schema import minio_schema, image_schema, csv_schema
from utils.udf_helpers import tokenize_vietnamese, upload_image, caption_generator, get_current_time

def csv_process_stream(stream):
    """Process streaming data from CSV files.
    
    Args:
        stream: Input streaming DataFrame.
        
    Returns:
        Processed streaming DataFrame with parsed JSON values.
    """
    value_schema = F.schema_of_json(minio_schema)
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), value_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def mobile_process_stream(stream):
    """Process streaming data from mobile devices.
    
    Args:
        stream: Input streaming DataFrame.
        
    Returns:
        Processed streaming DataFrame with parsed JSON values.
    """
    stream = (stream
                .selectExpr("CAST(value AS STRING)")
                .select(F.from_json(F.col("value"), image_schema).alias("data"))
                .select(F.col("data.*"))
             )
    return stream

def clean_caption(df, column):
    """Clean and tokenize caption text.
    
    Args:
        df: Input DataFrame.
        column: Name of column containing caption text.
        
    Returns:
        DataFrame with cleaned and tokenized captions.
    """
    regex_pattern = r'[!"""#$%&()*+,./:;<=>?@\[\\\]\^{|}~-]'
    df_cleaned = (df.withColumn(column, F.regexp_replace(F.col(column), regex_pattern, ""))
                    .withColumn(column, F.lower(F.col(column)))
                    .withColumn("caption_tokens", tokenize_vietnamese(F.col(column)))
                    .withColumn("tokenized_caption", F.concat_ws(' ', "caption_tokens"))
                    .withColumn("word_count", F.size("caption_tokens"))
                    .withColumn("created_time", get_current_time())
                 )
    return df_cleaned

def format_user_data(df):
    """Format user data with additional metadata.
    
    Args:
        df: Input DataFrame with user data.
        
    Returns:
        DataFrame with formatted user metadata.
    """
    formatted_df = (df.withColumn("original_url", F.concat(F.lit("http://160.191.244.13:9000/mlflow/user-data/images/"), F.col('image_name')))
                    .withColumn("source_website", F.lit("Mobile"))
                    .withColumn("search_query", F.lit("N/A"))
                    .withColumn("resolution", F.col('image_size'))
                    .withColumn("short_caption", F.lit(" "))
                    .withColumn("created_time", get_current_time())
                   )
    return formatted_df

def csv_process_batch(df, batch_id, spark, db_uri, gemini_key):
    """Process micro-batch of CSV data and write to MongoDB.
    
    Args:
        df: Input DataFrame with CSV data.
        batch_id: ID of current batch.
        spark: SparkSession object.
        db_uri: MongoDB connection URI.
        gemini_key: API key for Gemini service.
    """
    for row in df.collect():
        file_path = f"s3a://{row['Key']}"
        df_file = (spark.read
                    .option("delimiter", ",")
                    .option("header", True)
                    .option("encoding", "UTF-8")
                    .option("schema", csv_schema)
                    .csv(file_path, header=True)
                    .drop("local_path")
                    .dropDuplicates()
                    .dropna(subset="short_caption")
                  )
        
        df_cleaned = clean_caption(df_file, "short_caption")
        (df_cleaned.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", db_uri)
                .option("spark.mongodb.write.database", "imcp")
                .option("spark.mongodb.write.collection", "raw")
                .option("spark.mongodb.write.batch.size", "10000")
                .mode("append")
                .save()
        )
        
def mobile_process_batch(df, batch_id, spark, db_uri, gemini_key):
    """Process micro-batch of mobile data and write to MongoDB.
    
    Args:
        df: Input DataFrame with mobile data.
        batch_id: ID of current batch.
        spark: SparkSession object.
        db_uri: MongoDB connection URI.
        gemini_key: API key for Gemini service.
    """
    formatted_df = format_user_data(df)
    formatted_df = (formatted_df.withColumn("upload_status", upload_image(F.col("image_base64"), F.col("image_name")))
                                .withColumn("short_caption", caption_generator(F.lit(gemini_key), F.col("original_url")))
                                .drop("image_base64", "image_name", "image_size")
                   )
    df_cleaned = clean_caption(formatted_df, "short_caption")

    (df_cleaned.write
                .format("mongodb")
                .option("spark.mongodb.write.connection.uri", db_uri)
                .option("spark.mongodb.write.database", "imcp")
                .option("spark.mongodb.write.collection", "user_data")
                .option("spark.mongodb.write.batch.size", "10000")
                .mode("append")
                .save()
    )