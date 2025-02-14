import sys
sys.path.append("./work/imcp")

import pyspark.sql.functions as F
import pyspark.sql.types as T
from datetime import datetime
from utils.schema import csv_sample_schema, vehicle_map, traffic_status_map, street_map, environment_map, weather_map
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

def classify_categories(df):
    vehicle_col = F.array([F.lit(val) for val in vehicle_map])
    traffic_status_col = F.array([F.lit(val) for val in traffic_status_map])
    street_col = F.array([F.lit(val) for val in street_map])
    environment_col = F.array([F.lit(val) for val in environment_map])
    weather_col = F.array([F.lit(val) for val in weather_map])

    # Duyệt qua toàn bộ danh sách `caption_tokens`, ánh xạ từng từ với `vehicle_map_expr`
    df = (df
            .withColumn("vehicle_type", 
                        F.when(F.size(F.array_intersect(F.col("caption_tokens"), vehicle_col))==0, 
                               F.array(F.lit("khác")))
                        .otherwise(F.array_intersect(F.col("caption_tokens"), vehicle_col)))
            .withColumn("traffic_type",
                        F.when(F.size(F.array_intersect(F.col("caption_tokens"), traffic_status_col))==0,
                              F.array(F.lit("bình_thường")))
                        .otherwise(F.array_intersect(F.col("caption_tokens"), traffic_status_col)))
            .withColumn("street_type",
                        F.when(F.size(F.array_intersect(F.col("caption_tokens"), street_col))==0,
                              F.array(F.lit("đường_phố")))
                        .otherwise(F.array_intersect(F.col("caption_tokens"), street_col)))
            .withColumn("environment_type",
                        F.when(F.size(F.array_intersect(F.col("caption_tokens"), environment_col))==0,
                              F.array(F.lit("khác")))
                        .otherwise(F.array_intersect(F.col("caption_tokens"), environment_col)))
            .withColumn("weather_type",
                        F.when(F.size(F.array_intersect(F.col("caption_tokens"), weather_col))==0,
                              F.array(F.lit("khác")))
                        .otherwise(F.array_intersect(F.col("caption_tokens"), weather_col)))
           )
    return df

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