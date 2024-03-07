from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder \
    .appName("Read Single JSON") \
    .getOrCreate()

# Read single JSON file from S3 into DataFrame
df_single_json = spark.read.json("s3://ti-p-data/hr-data/emp_json/")

df_filtered = df_single_json.filter(~col("_corrupt_record").startswith("{") & ~col("_corrupt_record").endswith("}") &
                        ~(col("dept_id").isNull() & col("emp_id").isNull() &
                          col("emp_name").isNull() & col("join_date").isNull() &
                          col("salary").isNull()))

# Show the DataFrame
df_single_json.show()
