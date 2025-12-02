# Databricks notebook source
# MAGIC %md
# MAGIC ## BRONZE LAYER

# COMMAND ----------

import requests,json
from pyspark.sql import SparkSession

#defining the Spark Session
spark = SparkSession.builder.appName("JobETL_Bronze").getOrCreate()

#defining the API Key
API_KEY = "389429d5e4ce6553514c445f10f6ade53b4e34031a4c1c88f802ab97d22455be"
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER 

# COMMAND ----------

from pyspark.sql.functions import trim, upper, col
# Check the schema to confirm available columns
display(bronze_df)

# Use an existing column in dropna, for example "job_id"
silver_df = bronze_df.withColumn(
    "company_name",
    trim(upper(col("company_name")))
)
silver_df = silver_df.dropna(subset=["job_id"])

silver_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("default.silver_jobs")

display(spark.table("default.silver_jobs"))

# COMMAND ----------

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLDEN LAYER

# COMMAND ----------

from pyspark.sql.functions import *

print("✅Reading from Silver Layer -------> Golden Layer")

# Read from the correct table
df = spark.table("workspace.default.silver_jobs")

# Write to the correct gold table
df.write.format("delta").option("mergeSchema", "true").mode("append").saveAsTable("workspace.default.gold_jobs")

# KPI 1 - Top Companies
top_companies = df.groupBy("company_name").count().orderBy(col("count").desc())
display(top_companies)

# KPI - Jobs by City
jobs_by_city = df.groupBy('location').agg(count('*').alias('job_count')).orderBy(col('job_count').desc())
display(jobs_by_city)

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

spark = SparkSession.builder.appName("VisualsFromGold").getOrCreate()

# COMMAND ----------

# Load gold layer: top companies
df_companies = spark.read.table("workspace.default.gold_jobs")
# Remove or update the dropna line to use a valid column, e.g., "job_id"
df_companies = df_companies.dropna(subset=["job_id"])
display(df_companies)

# COMMAND ----------

df_companies = spark.read.table("workspace.default.gold_jobs")
# Convert to Pandas for plotting
df_companies = spark.read.table("workspace.default.gold_jobs")
pdf = (
    df_companies.orderBy("job_id", ascending=True)
    .limit(10)
    .toPandas()
)

# COMMAND ----------

display(pdf)

# COMMAND ----------

import matplotlib.pyplot as plt
df_cities = spark.read.table("workspace.default.gold_jobs")
# Remove dropna for 'posted_at' since it does not exist
pdf_city = df_cities.orderBy(
    "job_id",
    ascending=False
).toPandas()

plt.figure(figsize=(10, 6))
plt.barh(
    pdf_city['location'][::-1],
    pdf_city['title'][::-1],
    color='lightgreen'
)
plt.xlabel("Job Postings")
plt.title("Job Demand by City")
plt.grid(True, axis='x', linestyle='--', alpha=0.5)
plt.tight_layout()
plt.show()