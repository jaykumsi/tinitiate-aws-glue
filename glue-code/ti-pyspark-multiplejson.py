from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Read Multiple JSON Files from S3") \
    .getOrCreate()

# Specify the S3 bucket and path containing the JSON files
s3_bucket = "ti-p-data"
s3_path = "s3://ti-p-data/hr-data/multiple_json/"

# Read multiple JSON files from S3 into a DataFrame
df = spark.read.json(f"s3://ti-p-data/hr-data/multiple_json/*.json")

# Show the schema of the DataFrame
df.printSchema()

# Show the first few rows of the DataFrame
df.show()
