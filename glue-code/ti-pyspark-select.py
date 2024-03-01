## Load tables from Athena into data frames
product_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="product").toDF()
category_df = glueContext.create_dynamic_frame.from_catalog(database=database, table_name="category").toDF()

# DataFrame Alias

# Select variation
# Select specific columns from the tables to avoid duplicate column names
product_selected_df = product_df.select("productid", "productname", "categoryid", "unit_price").withColumnRenamed("categoryid", "product_categoryid")
category_selected_v1_df = category_df.select(product_df["categoryid"], product_df["categoryname"])
category_selected_v1_df = category_df.select(product_df.categoryid, product_df.categoryname)
category_selected_v2_df = new dataframe[].select(product_df.categoryid, product_df.categoryname)
category_selected_v3_df = new dataframe[].select(product_df["categoryid"], product_df["categoryname"])


# Join the tables on categoryid column