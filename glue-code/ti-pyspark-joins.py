from pyspark.context import SparkContext
from awsglue.context import GlueContext
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

# Select specific columns from the tables to avoid duplicate column names
product_selected_df = product_df.select("productid", "productname", "categoryid", "unit_price").withColumnRenamed("categoryid", "product_categoryid")
category_selected_df = category_df.select("categoryid", "categoryname")

# Join the tables on categoryid column
inner_df = product_selected_df.join(category_selected_df, product_selected_df["product_categoryid"] == category_selected_df["categoryid"], "inner")

#inner_df= inner_df.select("productid", "productname", "categoryid", "unit_price")

# Log the count of rows in the joined DataFrame
glueContext.get_logger().info("Number of rows in the inner joined DataFrame: {}".format(inner_df.count()))

# Show the resulting DataFrame
inner_df.show()

# Drop the category_categoryid column from the joined DataFrame
inner_df = inner_df.drop("product_categoryid")

# Show the resulting DataFrame after dropping the column
inner_df.show()

# Left Join the tables on categoryid column
left_df = product_selected_df.join(category_selected_df, product_selected_df["product_categoryid"] == category_selected_df["categoryid"], "left")

# Drop the category_categoryid column from the joined DataFrame
left_df = left_df.drop("product_categoryid")

# Show the resulting DataFrame
left_df.show()

# Right Join the tables on categoryid column
right_df = product_selected_df.join(category_selected_df, product_selected_df["product_categoryid"] == category_selected_df["categoryid"], "right")


# Drop the category_categoryid column from the joined DataFrame
right_df = right_df.drop("product_categoryid")


# Show the resulting DataFrame
right_df.show()

# Full Outer Join the tables on categoryid column
full_outer_df = product_selected_df.join(category_selected_df, product_selected_df["product_categoryid"] == category_selected_df["categoryid"], "outer")



# Drop the category_categoryid column from the joined DataFrame
full_outer_df = full_outer_df.drop("product_categoryid")



# Show the resulting DataFrame
full_outer_df.show()
