from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Read Single Parquet File") \
    .getOrCreate()

# S3 path to the Parquet file
parquet_path = "s3://ti-p-data/customer-billing/parquet/"

# Read the Parquet file into DataFrame
df = spark.read.parquet(parquet_path)

# Show DataFrame
df.show()

# Stop SparkSession
spark.stop()
