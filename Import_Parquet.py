# Databricks notebook source
# MAGIC %md
# MAGIC This Notebook imports the parquest files found here: https://github.com/JonIsAverage/NYC_Taxis/tree/main

# COMMAND ----------

import requests
import pandas as pd
import zipfile
import os

# COMMAND ----------

directory_path = "/dbfs/tmp/"

if not os.path.exists(directory_path):
    os.makedirs(directory_path)
    print(f"Directory {directory_path} created.")
else:
    print(f"Directory {directory_path} already exists.")

# COMMAND ----------

def main(var):
    github_url = f"https://raw.githubusercontent.com/JonIsAverage/NYC_Taxis/main/green_tripdata_2022-{str(var)}.parquet.zip"
    response = requests.get(github_url)

    if response.status_code == 200:
        print(f"Downloaded zip file size: {len(response.content)} bytes")
        target_directory = "/dbfs/tmp/"

        zip_file_path = target_directory + "data.zip"
        with open(zip_file_path, 'wb') as zip_file:
            zip_file.write(response.content)

        print(f"Zip file written to: {zip_file_path}")

        zip_file_path = "/dbfs/tmp/data.zip"

        extracted_directory = "/dbfs/tmp/"

        os.makedirs(extracted_directory, exist_ok=True)

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_directory)

# COMMAND ----------

for i in range(1, 13):
    var = "{:02d}".format(i)
    main(var)
    
extracted_directory = "/dbfs/tmp/"
extracted_files = os.listdir(extracted_directory)
print("Extracted Files:")
for file in extracted_files:
    print(file)

# COMMAND ----------

#DEBUG
#parquet_file_path = "/dbfs/tmp/green_tripdata_2022-02.parquet"
#df = pd.read_parquet(parquet_file_path, engine='pyarrow')
#display(df)
