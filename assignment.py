import sys
import logging
import yaml
import time
import boto3
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType

# --------------------------
# CONFIGURATION
# --------------------------
CONFIG_PATH = ".github/workflows/"


def load_config(path):
    with open(path, "r") as file:
        return yaml.safe_load(file)


config = load_config(CONFIG_PATH)

# --------------------------
# LOGGING SETUP
# --------------------------
logger = logging.getLogger("GlueETLJob")
logger.setLevel(logging.INFO)

# --------------------------
# GLUE CONTEXT SETUP
# --------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# --------------------------
# ETL FUNCTIONS
# --------------------------


def read_csv(glueContext, path):
    """Read CSV from S3 into a Glue DynamicFrame with automatic schema inference"""
    logger.info(f"Reading CSV files from {path}")
    return glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="csv",
        connection_options={"paths": [path]},
        format_options={"withHeader": True, "inferSchema": True}
    )


def infer_and_cast_types(df):
    """Infer types from data and cast columns"""
    for col_name, dtype in df.dtypes:
        if col_name in ["Index", "Customer_Id"]:
            df = df.withColumn(col_name, col(col_name).cast(IntegerType()))
        else:
            df = df.withColumn(col_name, col(col_name).cast(StringType()))
    return df


def transform_data_dynamic(dynamic_frame, fillna_dict=None):
    """Transform DynamicFrame: cast types, handle nulls, dedup"""
    df = dynamic_frame.toDF()
    df = infer_and_cast_types(df)

    if fillna_dict:
        df = df.fillna(fillna_dict)

    df = df.dropDuplicates()
    return DynamicFrame.fromDF(df, glueContext, "transformed_df")


def write_parquet(dynamic_frame, output_path):
    """Write DynamicFrame to Parquet in S3"""
    logger.info(f"Writing output to {output_path}")
    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="s3",
        connection_options={"path": output_path},
        format="parquet"
    )


def write_to_catalog(dynamic_frame, database, table):
    """Write data to Glue Data Catalog"""
    if database and table:
        logger.info(f"Writing data to Glue Catalog table {database}.{table}")
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=database,
            table_name=table,
            transformation_ctx="datasink"
        )

# --------------------------
# MAIN ETL JOB
# --------------------------


def main_etl():
    try:
        df = read_csv(glueContext, config["input_path"])
        transformed_df = transform_data_dynamic(
            df,
            fillna_dict=config.get("fillna_dict")
        )
        write_parquet(transformed_df, config["output_path"])
        write_to_catalog(
            transformed_df, config.get("database"), config.get("table")
        )
        logger.info("ETL job completed successfully!")
    except Exception as e:
        logger.error(f"ETL job failed: {str(e)}")
        raise

# --------------------------
# MAIN ENTRY POINT
# --------------------------


if __name__ == "__main__":
    try:
        # If you have a Glue Job name in config, you can trigger Glue job instead
        if config.get("glue_job_name"):
            glue = boto3.client(
                "glue", region_name=config.get("region", "us-east-1"))
            logger.info(f"Starting Glue job {config['glue_job_name']} ...")
            response = glue.start_job_run(JobName=config["glue_job_name"])
            job_run_id = response['JobRunId']
            logger.info(f"Glue Job started with Run ID: {job_run_id}")

            # Monitor job until it finishes
            while True:
                resp = glue.get_job_run(
                    JobName=config["glue_job_name"], RunId=job_run_id)
                status = resp['JobRun']['JobRunState']
                logger.info(f"Current Status: {status}")
                if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    logger.info(f"Final Status: {status}")
                    break
                time.sleep(30)
        else:
            # Run ETL locally
            main_etl()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
