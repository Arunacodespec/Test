import sys
import logging
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType
import argparse

# --------------------------
# Logging Setup
# --------------------------
logger = logging.getLogger("GlueETLJob")
logger.setLevel(logging.INFO)

# --------------------------
# Glue Context Setup
# --------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------------
# Argument Parser
# --------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--INPUT_PATH", required=True)
parser.add_argument("--OUTPUT_PATH", required=True)
parser.add_argument("--DATABASE", required=False)
parser.add_argument("--TABLE", required=False)
parser.add_argument("--FILLNA_CUSTOMER_NAME", default="Unknown")
parser.add_argument("--FILLNA_EMAIL", default="Unknown")
args = parser.parse_args()

# --------------------------
# ETL Functions
# --------------------------


def read_csv(path):
    logger.info(f"Reading CSV from {path}")
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path]},
        format_options={"withHeader": True, "inferSchema": True}
    )


def transform_data(df):
    df_spark = df.toDF()
    # Cast types
    for col_name, dtype in df_spark.dtypes:
        if col_name in ["Index", "Customer_Id"]:
            df_spark = df_spark.withColumn(
                col_name, col(col_name).cast(IntegerType()))
        else:
            df_spark = df_spark.withColumn(
                col_name, col(col_name).cast(StringType()))
    # Fill nulls
    df_spark = df_spark.fillna({
        "Customer_Name": args.FILLNA_CUSTOMER_NAME,
        "Email": args.FILLNA_EMAIL
    })
    # Drop duplicates
    df_spark = df_spark.dropDuplicates()
    return DynamicFrame.fromDF(df_spark, glueContext, "transformed_df")


def write_parquet(dynamic_frame, output_path):
    logger.info(f"Writing Parquet to {output_path}")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )


def write_catalog(dynamic_frame, database, table):
    if database and table:
        logger.info(f"Writing to Glue Catalog: {database}.{table}")
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database,
            table_name=table
        )


# --------------------------
# Main ETL
# --------------------------
try:
    df_raw = read_csv(args.INPUT_PATH)
    df_transformed = transform_data(df_raw)
    write_parquet(df_transformed, args.OUTPUT_PATH)
    write_catalog(df_transformed, args.DATABASE, args.TABLE)
    logger.info("ETL job completed successfully!")
except Exception as e:
    logger.error(f"ETL job failed: {str(e)}")
    sys.exit(1)
