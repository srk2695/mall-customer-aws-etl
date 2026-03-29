from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder.appName("MallCustomerETL").getOrCreate()

# Read data from S3
df = spark.read.csv(
    "s3://mall-customer-data-lake/raw/Mall_Customers.csv",
    header=True,
    inferSchema=True
)

# Rename columns
df = df.withColumnRenamed("Annual Income (k$)", "annual_income") \
       .withColumnRenamed("Spending Score (1-100)", "spending_score")

# Create spending segments
df = df.withColumn(
    "spending_segment",
    when(col("spending_score") < 40, "Low")
    .when(col("spending_score").between(40, 70), "Medium")
    .otherwise("High")
)

# Write to processed layer
df.write.mode("overwrite").parquet(
    "s3://mall-customer-data-lake/processed/"
)