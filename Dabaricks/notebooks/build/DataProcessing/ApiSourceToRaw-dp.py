# Databricks notebook source
import json
import requests
import webbrowser
import base64
import os
from pyspark.sql.functions import lit
import pyspark.sql.functions as F
from pprint import pprint as pp    # not required for making requests; pretty printing

# COMMAND ----------

# MAGIC %run "/build/Util/BlobHelper-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/FlattenJSON-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/ApiUtil-ut.py"

# COMMAND ----------

# MAGIC %run "/build/Util/ApiBlobHelper-ut.py"

# COMMAND ----------

# MAGIC %md
# MAGIC #Parameters

# COMMAND ----------

# Fred

dbutils.widgets.text('srcKvSecret','','')
dbutils.widgets.text('dstKvSecret','','')
#dbutils.widgets.text('srcAccount','','')
dbutils.widgets.text('dstAccount','','')
dbutils.widgets.text('srcContainerName','','')
dbutils.widgets.text('srcDirectoryName','','')
dbutils.widgets.text('srcType','','')
dbutils.widgets.text('dstType','','')
dbutils.widgets.text('dstContainerName','','')
dbutils.widgets.text('dstDirectoryName','','')
dbutils.widgets.text('srcBlobName','','')
dbutils.widgets.text('dstBlobName','','')
dbutils.widgets.text('dstTableName','','')
dbutils.widgets.text('srcFormat','','')
dbutils.widgets.text('dstFormat','','')
dbutils.widgets.text('prcType','','')
dbutils.widgets.text('metadataquery','','')
dbutils.widgets.text('samplejsonstructure','','')
dbutils.widgets.text('sourcequery','','')
dbutils.widgets.text('sourceendpoint','','')
dbutils.widgets.text('sourceconfig','','')
dbutils.widgets.text('ifModifiedSince','','')

parameters = dict(
     srcKvSecret = dbutils.widgets.get('srcKvSecret')
    #,srcAccName = dbutils.widgets.get('srcAccount')
    ,srcContainerName = dbutils.widgets.get('srcContainerName')
    ,srcDirectoryName = dbutils.widgets.get('srcDirectoryName')
    ,srcType = dbutils.widgets.get('srcType')
    ,dstType = dbutils.widgets.get('dstType')
    ,dstKvSecret = dbutils.widgets.get('dstKvSecret')
    ,dstAccName = dbutils.widgets.get('dstAccount')
    ,dstContainerName = dbutils.widgets.get('dstContainerName')
    ,dstDirectoryName = dbutils.widgets.get('dstDirectoryName')
    ,srcBlobName = dbutils.widgets.get('srcBlobName')
    ,dstBlobName = dbutils.widgets.get('dstBlobName')
    ,dstTableName = dbutils.widgets.get('dstTableName')
    ,srcFormat = dbutils.widgets.get('srcFormat')
    ,dstFormat = dbutils.widgets.get('dstFormat')
    ,prcType = dbutils.widgets.get('prcType')
    ,metadataquery = dbutils.widgets.get('metadataquery')
    ,samplejsonstructure = dbutils.widgets.get('samplejsonstructure')
    ,sourcequery = dbutils.widgets.get('sourcequery')
    ,sourceendpoint = dbutils.widgets.get('sourceendpoint')
    ,sourceconfig = dbutils.widgets.get('sourceconfig')
    ,ifModifiedSince = dbutils.widgets.get('ifModifiedSince')
)

# COMMAND ----------

config_json = json.loads(dbutils.secrets.get(scope='secretScope',key=parameters["srcKvSecret"]))
source_app = config_json["Source_App"]
if source_app == 'Api':
  authentication_method = config_json["Authentication_Method"]
  api_user_name = config_json["User_Name"]
  api_password = config_json["Password"]
  end_point_url = config_json["End_Point_URL"]   
table_name = parameters["srcDirectoryName"] 
dstAccName = parameters["dstAccName"]
dstContainerName = parameters["dstContainerName"]
dstDirectoryName = parameters["dstDirectoryName"]
dstBlobName = parameters["dstBlobName"]
dstFormat = parameters["dstFormat"]
srcFormat = parameters["srcFormat"]
dstKvSecret = parameters["dstKvSecret"]
meta_data_query = parameters["metadataquery"]

# COMMAND ----------

dstBlobPath = GetBlobStoreFiles(parameters["dstKvSecret"],parameters["dstAccName"],\
                                parameters["dstContainerName"],parameters["dstDirectoryName"],parameters["dstBlobName"],'azure data lake store')
print("Destination Blob Path = " + dstBlobPath)

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformation

# COMMAND ----------

def transform():
 if source_app == 'Api':
    master_df = get_api_master_df(json.loads(parameters["samplejsonstructure"]), parameters["sourcequery"])
    api_response_df = get_api_master_df(api_user_name, api_password, end_point_url, parameters["sourceendpoint"], master_df, parameters["sourcequery"], parameters["ifModifiedSince"])
    master_df = api_response_df
    save_csv(dstBlobPath, master_df, dstFormat)
    file_name = table_name
    rename_csv_files(dstBlobPath, dstFormat, file_name)  
  print("Master DF Record Count = " + str(master_df.count()))

# COMMAND ----------

transform()