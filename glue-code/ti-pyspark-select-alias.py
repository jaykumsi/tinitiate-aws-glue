from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from awsglue.transforms import *
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark context with log level
sc = SparkContext()
sc.setLogLevel("INFO")  # Setting log level for Spark context

glueContext = GlueContext(sc)

# Define Athena catalog and database
catalog = "awsglue_data_catalog"
database = "glue_db"

# Load tables from Athena into data frames
product_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="product").toDF()
category_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="category").toDF()

# Select and rename specific columns from the product table
product_selected_df = product_df.select(
    product_df["productid"].alias("id"),
    product_df["productname"].alias("name"),
    product_df["categoryid"].alias("category_id"),
    product_df["unit_price"].alias("price")
)

# Select and rename specific columns from the category table
category_selected_df = category_df.select(
    category_df["categoryid"].alias("id"),
    category_df["categoryname"].alias("name")
)

# Show the resulting DataFrames
product_selected_df.show()
category_selected_df.show()
