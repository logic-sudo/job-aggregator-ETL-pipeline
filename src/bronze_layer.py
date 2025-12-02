# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE LAYER

# COMMAND ----------

import requests,json
from pyspark.sql import SparkSession

#defining the Spark Session
spark = SparkSession.builder.appName("JobETL_Bronze").getOrCreate()

#defining the API Key
API_KEY = "ENTER YOUR SERP API KEY"
roles = ["Data Engineer", "Python Developer", "ETL Developer", "Spark Engineer", "Data Analyst"]
location = "India"
all_jobs = []

#Running a Loop over all the Job Roles
for role in roles:
    params={
        "engine":"google_jobs",
        "q":role,
        "location":location,
        "api_key":API_KEY
    }
    res = requests.get("https://serpapi.com/search.json", params=params)
    print("✅Reading 🔴LIVE Data from Google Jobs API")
    jobs = res.json().get("jobs_results", [])
                        
    for job in jobs:
        job["search_role"] = role
    all_jobs.extend(jobs)

# Convert jobs list directly to Spark DataFrame
bronze_df = spark.createDataFrame(all_jobs)

bronze_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("default.bronze_jobs_raw")

print("✅Data Written to Bronze Layer")

# COMMAND ----------

display(bronze_df)
