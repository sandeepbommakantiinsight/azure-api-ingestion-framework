# Databricks notebook source
import json
import requests
import base64
import math
import time
from pyspark.sql.functions import lit
from pyspark.sql.types import *
from pyspark.sql.types import StructType, StructField
from pyspark.sql import Row
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #Api Basic Authentication

# COMMAND ----------

def api_main_request(baseurl, endpoint, api_user_name, api_password, params = None):
  headers = requests.utils.default_headers()
  headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36'
  payload = params
  r = requests.get(baseurl + endpoint, auth=(api_user_name, api_password), headers=headers, params=payload)  
  return r

# COMMAND ----------

# MAGIC %md
# MAGIC #Get Api Master DataFrame

# COMMAND ----------

#Get Master DataFrame
def get_api_master_df(sample_json, query):
  sample_df = sc.parallelize([sample_json]).map(lambda x: json.dumps(x))
  sample_df = spark.read \
              .option("multiLine", True) \
              .option("mode", "PERMISSIVE") \
              .json(sample_df)
  master_schema = sample_df.schema
  emptyRDD = spark.sparkContext.emptyRDD()
  master_df = spark.createDataFrame(emptyRDD,master_schema)
  return master_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Get API Data

# COMMAND ----------

def get_api_df (request_json, master_df, source_query):
  json_data = request_json.json()
  api_df = sc.parallelize(json_data).map(lambda x: json.dumps(x))
  api_df = spark.read \
           .option("multiLine", True) \
           .option("mode", "PERMISSIVE") \
           .json(api_df)
  full_column_df = spark.createDataFrame(api_df.rdd, schema = master_df.schema)
  full_column_df.createOrReplaceTempView("final_tab")
  final_df = spark.sql(source_query.format("final_tab"))
  return final_df

# COMMAND ----------

# MAGIC %md
# MAGIC #Api Transformation Master

# COMMAND ----------

def api_transformation_master(api_user_name, api_password, end_point_url, sourceendpoint, master_df, source_query, if_modified_since = None):  
  retry_attempt = 0
  request_json = api_main_request(end_point_url, sourceendpoint, api_user_name, api_password, if_modified_since)  
  print(request_json.status_code)
  #print(request_json.text)
  if request_json.status_code == 200 and retry_attempt <= 10:
    data_df = get_api_df (request_json, master_df, source_query)  
  else:
    retry_attempt = retry_attempt + 1
    data_df = get_api_df (request_json, master_df, source_query)  
  return data_df
