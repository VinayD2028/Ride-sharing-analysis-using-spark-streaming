from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Create a Spark session
spark = SparkSession.builder.appName("RideSharingAnalytics").getOrCreate()

# Define the schema for incoming JSON data
schema = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("distance_km", DoubleType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Read streaming data from socket
raw_stream = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Parse JSON data into columns using the defined schema
parsed_stream = raw_stream.select(from_json(col("value"), schema).alias("data")).select("data.*")

query = parsed_stream.writeStream.format("csv").option("header", "true").option("path", "output/task_1.csv").option("checkpointLocation", "checkpoints/ride_data").outputMode("append").start()


# Print parsed data to the console
#query = parsed_stream.writeStream.format("console").outputMode("append").start()
query.awaitTermination()
