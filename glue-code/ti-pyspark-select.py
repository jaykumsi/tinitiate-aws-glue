from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
import logging

# Create SparkSession
spark = SparkSession.builder.appName("Select Columns").getOrCreate()

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

# Select specific columns from product table
product_selected_df = product_df.select(
    product_df["productid"].alias("productid"),
    product_df["productname"].alias("productname"),
    product_df["categoryid"].alias("product_categoryid"),
    product_df["unit_price"]
)

# Select specific columns from category table
category_selected_df = category_df.select(
    category_df["categoryid"].alias("categoryid"),
    category_df["categoryname"]
)

# Join product and category tables on categoryid column
joined_df = product_selected_df.join(
    category_selected_df,
    product_selected_df["product_categoryid"] == category_selected_df["categoryid"],
    "inner"
)

# Drop the duplicate categoryid column
joined_df = joined_df.drop("categoryid")

# Show the resulting DataFrame
joined_df.show()
