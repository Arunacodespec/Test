from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType

# Init Spark / Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input / Output paths
INPUT_CSV_PATH = "s3://my-input-bucket/github/sample_data.csv"
OUTPUT_PARQUET_PATH = "s3://my-curated-bucket/github_sample_data/"

# Read CSV
df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(INPUT_CSV_PATH)
)

# Limit to 100 records
df_100 = df.limit(100)

# Basic cleanup
df_clean = (
    df_100
    .withColumn("id", col("id").cast(IntegerType()))
    .filter(col("id").isNotNull())
)

# Write Parquet to S3
(
    df_clean.write
    .mode("overwrite")              # use append for incremental loads
    .format("parquet")
    .option("compression", "snappy")
    .save(OUTPUT_PARQUET_PATH)
)

print("✅ Parquet written to S3")
