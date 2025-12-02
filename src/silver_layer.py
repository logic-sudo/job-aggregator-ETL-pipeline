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
