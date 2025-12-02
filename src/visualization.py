from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

def get_spark():
    return SparkSession.builder.appName("VisualsFromGold").getOrCreate()

def plot_top_companies(spark):
    """
    Plots the top 10 companies from the Gold layer.
    """
    df_companies = spark.read.table("workspace.default.gold_jobs")

    # Remove rows where job_id is null (prevents sorting issues)
    df_companies = df_companies.dropna(subset=["job_id"])

    # Convert to Pandas for visualization
    pdf = (
        df_companies.orderBy("job_id", ascending=True)
        .limit(10)
        .toPandas()
    )

    plt.figure(figsize=(10, 5))
    plt.barh(pdf["company_name"], pdf["job_id"], color="skyblue")
    plt.xlabel("Job ID (ascending)")
    plt.title("Top 10 Companies with Earliest Job IDs")
    plt.tight_layout()
    plt.show()


def plot_jobs_by_city(spark):
    """
    Plots job demand by city using a bar chart.
    """
    df_cities = spark.read.table("workspace.default.gold_jobs")

    pdf_city = (
        df_cities.orderBy("job_id", ascending=False)
        .toPandas()
    )

    plt.figure(figsize=(10, 6))
    plt.barh(
        pdf_city["location"][::-1],
        pdf_city["job_id"][::-1],
        color="lightgreen"
    )
    plt.xlabel("Job ID (descending)")
    plt.title("Job Demand by City (Based on Job IDs)")
    plt.grid(True, axis="x", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()


def main():
    spark = get_spark()
    plot_top_companies(spark)
    plot_jobs_by_city(spark)
    spark.stop()

if __name__ == "__main__":
    main()
