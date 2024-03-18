# Databricks notebook source
# MAGIC %md
# MAGIC # Senior Data Engineer Task @ PEI
# MAGIC ##### Scenario: E-commerce Sales Data Processing with Databricks
# MAGIC ##### Source Datasets:
# MAGIC - Order.json: https://easyupload.io/4utl2u
# MAGIC - Customer.xlsx: https://easyupload.io/rsv257
# MAGIC - Product.csv: https://easyupload.io/4z1nlx
# MAGIC
# MAGIC **Note**: Follow a test-driven development (TDD) approach. Write appropriate test cases to ensure the correctness of the aggregations for the given scenarios

# COMMAND ----------

# DBTITLE 1,Datasets for PEI on FileStore Directory
# MAGIC %fs
# MAGIC ls /FileStore/PEI

# COMMAND ----------

# DBTITLE 1,Import code from Test_NB (contains test-utilities)
# MAGIC %run ./Test_NB

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create raw tables for each source dataset

# COMMAND ----------

# DBTITLE 1,Read datasets into pyspark data frames
raw_orderDF = spark.read.option("multiLine","true").json("dbfs:/FileStore/PEI/Order.json")
raw_productDF = spark.read.option("header","true").csv("dbfs:/FileStore/PEI/Product.csv")
raw_customerDF = spark.read.format("com.crealytics.spark.excel").option("header","true").load("dbfs:/FileStore/PEI/Customer.xlsx")

# COMMAND ----------

# DBTITLE 1,Write to datasets into tables
# Note : I have used parquet in this scenario as there were column names with special character/ invalid character(s) which are not supported directly for delta table creation
raw_customerDF.write.format("parquet").mode("overwrite").saveAsTable("customers")
raw_productDF.write.format("parquet").mode("overwrite").saveAsTable("products")
raw_orderDF.write.format("parquet").mode("overwrite").saveAsTable("orders")

# COMMAND ----------

# DBTITLE 1,Check if the tables are created
check_tbls = ("customers", "products", "orders")
tbl_existence(check_tbls)

# COMMAND ----------

orders = raw_orderDF.alias("orders")
products = raw_productDF.alias("products")
customers = raw_customerDF.alias("customers")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create an enriched table for customers and products 

# COMMAND ----------

# DBTITLE 1,Create Master table
# create master table to be used further in data transformations followed.
orders.join(products, orders["Product ID"]==products["Product ID"], "left")\
.join(customers, orders["Customer ID"]==customers["Customer ID"], "left")\
.withColumn("CustomerID", F.col("orders.`Customer ID`"))\
.withColumn("productID", F.col("products.`Product ID`"))\
.withColumn("product_state", F.col("products.`State`"))\
.withColumn("customer_state", F.col("customers.`State`"))\
.drop(F.col("customers.`Customer ID`"), F.col("products.`Product ID`"), F.col("products.`State`"), F.col("customers.`State`"))\
.select("orders.*", "products.*", "customers.*").write.format("parquet").mode("overwrite").saveAsTable("master_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create an enriched table which has
# MAGIC - Order information 
# MAGIC   - Profit rounded to 2 decimal places
# MAGIC - Customer name and country
# MAGIC - Product category and sub category

# COMMAND ----------

spark.read.table("master_raw")\
.select(
  "Order ID",
  F.round("Profit", 2).alias("Profit"),
  "Customer Name",
  "Country",
  "Category",
  "Sub-Category"
)\
.distinct()\
.write.format("parquet").mode("overwrite").saveAsTable("enriched_orderinfo")

# COMMAND ----------

tst_eo = Test_Enriched_OrderInfo()
execute_methods(tst_eo)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create an aggregate table that shows profit by 
# MAGIC - Year
# MAGIC - Product Category
# MAGIC - Product Sub Category
# MAGIC - Customer

# COMMAND ----------

spark.read.table("master_raw")\
.withColumn("Year", F.year(F.to_date("Order Date",'d/m/y')))\
.groupBy(
  "Year",
  "Category",
  "Sub-Category",
  "Customer ID")\
.agg(F.sum("Profit").alias("Total Profit")
).write.format("parquet").mode("overwrite").saveAsTable("profit_agg")

# COMMAND ----------

tst_pa = Test_Profit_Agg()
execute_methods(tst_pa)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Using SQL output the following aggregates
# MAGIC - Profit by Year
# MAGIC - Profit by Year + Product Category
# MAGIC - Profit by Customer
# MAGIC - Profit by Customer + Year

# COMMAND ----------

# DBTITLE 1,Profit by Year
# MAGIC %sql
# MAGIC select sum(Profit) as `Profit`, year(to_date(`Order Date`, 'd/m/y')) as `Year`
# MAGIC from master_raw
# MAGIC group by `Year`

# COMMAND ----------

# DBTITLE 1,Profit by Year + Product Category
# MAGIC %sql
# MAGIC select sum(Profit) as `Profit`, year(to_date(`Order Date`, 'd/m/y')) as `Year`, Category
# MAGIC from master_raw
# MAGIC group by `Year`, Category

# COMMAND ----------

# DBTITLE 1,Profit by Customer
# MAGIC %sql
# MAGIC select sum(Profit) as `Profit`, `Customer Name`
# MAGIC from master_raw
# MAGIC group by `Customer Name`

# COMMAND ----------

# DBTITLE 1,Profit by Customer + Year
# MAGIC %sql
# MAGIC select sum(Profit) as `Profit`, year(to_date(`Order Date`, 'd/m/y')) as `Year`, `Customer Name`
# MAGIC from master_raw
# MAGIC group by `Year`, `Customer Name`