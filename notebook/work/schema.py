from pyspark.sql.types import IntegerType, StringType, DoubleType, TimestampType, StructType, StructField

schema_taxi = StructType([
    StructField("vendorId", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_location", IntegerType(), True),
    StructField("dropoff_location", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("pickup_datetime", TimestampType(), True),
])