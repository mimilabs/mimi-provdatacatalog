# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Doctors and Clinicians Files
# MAGIC
# MAGIC There are many files, but we focus on two:
# MAGIC
# MAGIC - DAC_NationalDownloadableFile.csv
# MAGIC - Facility_Affiliation.csv

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/provdatacatalog/src/doctors_and_clinicians" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "provdatacatalog" # delta table destination schema


# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## NDF (National Downloadable File)

# COMMAND ----------

tablename = "dac_ndf" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for folderpath in Path(f"{path}").glob("*"):
    if folderpath.stem < '2023_01':
        continue
    for filepath in Path(f"{folderpath}").glob("DAC_NationalDownloadableFile*"):
        year = folderpath.stem[:4]
        month = folderpath.stem[5:]
        dt = parse(f"{year}-{month}-01").date()
        if dt not in files_exist:
            files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

int_columns = {"grd_yr", "num_org_mem"}
legacy_columns = {"lst_nm": "provider_last_name",
                  "frst_nm": "provider_first_name",
                  "mid_nm": "provider_middle_name",
                  "org_nm": "facility_name",
                  "cty": "citytown",
                  "st": "state",
                  "zip": "zip_code",
                  "phn_numbr": "telephone_number"}
for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new_ in zip(df.columns, change_header(df.columns)):
        
        col_new = legacy_columns.get(col_new_, col_new_)
        header.append(col_new)
        
        if col_new in int_columns:
            df = df.withColumn(col_new, regexp_replace(col(col_old), "[$,%]", "").cast("int"))
        else:
            df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Facility Affiliations

# COMMAND ----------

tablename = "dac_fa" # destination table

# COMMAND ----------

# We want to skip those files that are already in the delta tables.
# We look up the table, and see if the files are already there or not.
files_exist = {}
writemode = "overwrite"
if spark.catalog.tableExists(f"{catalog}.{schema}.{tablename}"):
    files_exist = set([row["_input_file_date"] 
                   for row in 
                   (spark.read.table(f"{catalog}.{schema}.{tablename}")
                            .select("_input_file_date")
                            .distinct()
                            .collect())])
    writemode = "append"

# COMMAND ----------

files = []
for folderpath in Path(f"{path}").glob("*"):
    if folderpath.stem < '2023_01':
        continue
    for filepath in Path(f"{folderpath}").glob("Facility_Affiliation*"):
        year = folderpath.stem[:4]
        month = folderpath.stem[5:]
        dt = parse(f"{year}-{month}-01").date()
        if dt not in files_exist:
            files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

for item in files:
    # each file is relatively big
    # so we load the data using spark one by one just in case
    # the size hits a single machine memory
    df = (spark.read.format("csv")
            .option("header", "true")
            .load(str(item[1])))
    header = []
    for col_old, col_new in zip(df.columns, change_header(df.columns)):
        header.append(col_new)
        df = df.withColumn(col_new, col(col_old))
            
    df = (df.select(*header)
          .withColumn("_input_file_date", lit(item[0])))
    
    # Some of these are manually corrected above. However, just in case
    # we make the mergeSchema option "true"
    (df.write
        .format('delta')
        .mode(writemode)
        .option("mergeSchema", "true")
        .saveAsTable(f"{catalog}.{schema}.{tablename}"))
    
    writemode="append"

# COMMAND ----------


