from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

# Init Spark / Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Paths
INPUT_CSV_PATH = "s3://my-input-bucket/customers/customers.csv"
OUTPUT_PARQUET_PATH = "s3://my-curated-bucket/customers_parquet/"

# Read CSV from S3
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_CSV_PATH)
)

# Limit to 100 records
df_100 = df.limit(100)

# Explicit schema cleanup (best practice)
df_clean = (
    df_100
    .withColumn("Index", col("Index").cast(IntegerType()))
    .withColumn("Customer_Id", col("Customer_Id").cast(IntegerType()))
    .withColumn("First_Name", col("First_Name").cast(StringType()))
    .withColumn("Last_Name", col("Last_Name").cast(StringType()))
    .withColumn("Company", col("Company").cast(StringType()))
    .withColumn("City", col("City").cast(StringType()))
    .withColumn("Country", col("Country").cast(StringType()))
)

# Write Parquet (Snappy compressed)
(
    df_clean.write
    .mode("overwrite")        # change to append for incremental
    .format("parquet")
    .option("compression", "snappy")
    .save(OUTPUT_PARQUET_PATH)
)

print("✅ Customer Parquet data written to S3")
