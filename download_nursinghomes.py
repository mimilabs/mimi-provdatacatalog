# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # National Downloadable Files for Doctors and Clinicians

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

!pip install tqdm

# COMMAND ----------

import requests
from pathlib import Path
from tqdm import tqdm
import datetime
from dateutil.relativedelta import *
import zipfile

# COMMAND ----------

# MAGIC %md
# MAGIC ## Doctors - National Downloadable Files

# COMMAND ----------

url = "https://data.cms.gov/provider-data/sites/default/files/archive/Nursing%20homes%20including%20rehab%20services/"
volumepath_root = "/Volumes/mimi_ws_1/provdatacatalog/src"
volumepath_zip = f"{volumepath_root}/zipfiles"
retrieval_range = 40 # in months, we wanted to get the data starting 2021-01-01; monthly update started in 2021

# COMMAND ----------

ref_monthyear = datetime.datetime.now()
files_to_download = []
for mon_diff in range(0, retrieval_range): 
    monthyear = (ref_monthyear - relativedelta(months=mon_diff)).strftime('%m_%Y')
    files_to_download.append(f"nursing_homes_including_rehab_services_{monthyear}.zip")

# COMMAND ----------

def download_file(url, filename, folder):
    # NOTE the stream=True parameter below
    with requests.get(f"{url}", stream=True) as r:
        r.raise_for_status()
        with open(f"{folder}/{filename}", 'wb') as f:
            for chunk in tqdm(r.iter_content(chunk_size=8192)): 
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                #if chunk: 
                f.write(chunk)

# COMMAND ----------

# Display the zip file links
for filename in files_to_download:
    # Check if the file exists
    if Path(f"{volumepath_zip}/{filename}").exists():
        # print(f"{filename} exists, skipping...")
        continue
    else:
        print(f"{filename} downloading...")
        year = filename[-8:-4]
        url_download = f"{url}/{year}/{filename}"
        download_file(url_download, filename, volumepath_zip)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unzip the downloaded files

# COMMAND ----------

files_downloaded = [x for x in Path(volumepath_zip).glob("nursing_homes_including_rehab_services*")]

# COMMAND ----------

for file_downloaded in files_downloaded:
    with zipfile.ZipFile(file_downloaded, "r") as zip_ref:
        year = file_downloaded.stem[-4:]
        month = file_downloaded.stem[-7:-5]
        for member in zip_ref.namelist():
            if (not Path(f"{volumepath_root}/nursing_homes/{year}_{month}/{member}").exists()):
                print(f"Extracting {member}...")
                try: 
                    zip_ref.extract(member, path=f"{volumepath_root}/nursing_homes/{year}_{month}")
                except NotImplementedError:
                    print(f"skipping {file_downloaded} due to an error...")

# COMMAND ----------


