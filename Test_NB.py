# Databricks notebook source
from pyspark.sql import functions as F

# COMMAND ----------

def get_table_list():
  return [tbl.name for tbl in spark.catalog.listTables()]

# COMMAND ----------

def tbl_existence(tbls):
  print("from Test_NB....")
  tbl_lst = get_table_list()
  for tbl in tbls:
    assert tbl in tbl_lst, f"Table: {tbl} is not created"
  
  return f"All tables are available in spark-catalog"

# COMMAND ----------

def get_methods(obj):
  return [m for m in dir(obj) if (not m.startswith('__')) and (callable(getattr(obj,m)))]

# COMMAND ----------

def execute_methods(obj):
  methods = get_methods(obj)
  for m in methods:
    try:
      res = getattr(obj,m)()
      print(res)
    except Exception as e:
      raise e

# COMMAND ----------

class Test_Enriched_OrderInfo:
  def __init__(self):
    self.tbl_name = "enriched_orderinfo"
  
  def exists(self):
    tbl_lst = get_table_list()
    assert self.tbl_name in tbl_lst, f"Table: {self.tbl_name} is not created"
    return f"{self.tbl_name} is available"
  
  def match_counts(self):
    cnt = spark.sql(f"select count(*) as cnt from {self.tbl_name}").rdd.keys().first()
    assert cnt == 9992, f"Counts do not match, {cnt}!=9992"
    return "Counts are matching"
  
  def test_cond(self):
    assert spark.read.table(f"{self.tbl_name}").filter("`Order ID`=='CA-2017-104640' and Category=='Technology'").select("profit").rdd.keys().first()==12452, f"Profit value does not match for `Order ID`: 'CA-2017-104640' and `Category`: 'Technology'"
    return "Profit value is as expected"

# COMMAND ----------

class Test_Profit_Agg:
  def __init__(self):
    self.tbl_name = "profit_agg"
  
  def exists(self):
    tbl_lst = get_table_list()
    assert self.tbl_name in tbl_lst, f"Table: {self.tbl_name} is not created"
    return f"{self.tbl_name} is available"
  
  def match_counts(self):
    cnt = spark.sql(f"select count(*) as cnt from {self.tbl_name}").rdd.keys().first()
    assert cnt == 8129, f"Counts do not match, {cnt}!=8129"
    return "Counts are matching"
  
  def test_cond(self):
    assert spark.read.table(f"{self.tbl_name}").groupBy("year").agg(F.count("`Customer ID`").alias("customer_count")).filter("year==2016").first().asDict()["customer_count"] == 2058, f"Customer Count does not match for year 2016"
    return "Customer Count matched"