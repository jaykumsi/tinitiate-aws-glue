from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
grouped_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="electric_vechiles").toDF()

# Group by electric vehicle column and count the occurrences
result_df = grouped_df.groupBy("make", "model").agg(count("*").alias("count"))

# Add ORDER BY clause to the DataFrame
result_df = result_df.orderBy("count", ascending=False)

# Show the resulting DataFrame
result_df.show()
