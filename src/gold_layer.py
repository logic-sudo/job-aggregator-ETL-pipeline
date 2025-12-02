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
