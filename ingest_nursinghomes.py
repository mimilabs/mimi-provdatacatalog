# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the Nursing Home files

# COMMAND ----------

from pathlib import Path
import re
import csv
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import col, lit, to_date, regexp_replace
from datetime import datetime
from dateutil.parser import parse
import pandas as pd

path = "/Volumes/mimi_ws_1/provdatacatalog/src/nursing_homes" # where all the input files are located
catalog = "mimi_ws_1" # delta table destination catalog
schema = "provdatacatalog" # delta table destination schema


# COMMAND ----------

def change_header(header_org):
    return [re.sub(r'\W+', '', column.lower().replace(' ','_'))
            for column in header_org]

# COMMAND ----------

# MAGIC %md
# MAGIC ## MDS Files

# COMMAND ----------

tablename = "nursinghomes_mds" # destination table

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
    #if folderpath.stem < '2023_01':
    #    continue
    for filepath in Path(f"{folderpath}").glob("NH_QualityMsr_MDS*"):
        year = folderpath.stem[:4]
        month = folderpath.stem[5:]
        dt = parse(f"{year}-{month}-01").date()
        if dt not in files_exist:
            files.append((dt, filepath))
files = sorted(files, key=lambda x: x[0], reverse=True)

# COMMAND ----------

double_columns = {"q1_measure_score", 
                  "q2_measure_score", 
                  "q3_measure_score", 
                  "q4_measure_score",
                  "four_quarter_average_score"}
date_columns = {"processing_date"}
legacy_columns = {}

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
        
        if col_new in double_columns:
            df = df.withColumn(col_new, col(col_old).cast("double"))
        elif col_new in date_columns:
            df = df.withColumn(col_new, to_date(col(col_old), format="yyyy-MM-dd"))
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


