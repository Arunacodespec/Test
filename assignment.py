import boto3
import time
import sys

# Configuration
REGION = "us-east-1"
GLUE_JOB_NAME = "my_etl_job"
S3_BUCKET = "fin-platform-raw-dev"
S3_KEY = "s3://fin-platform-raw-dev/raw/customers/customers.csv"
LOCAL_FILE = "customers.csv"

def upload_to_s3():
    s3 = boto3.client("s3", region_name=REGION)
    print("Uploading file to S3...")
    s3.upload_file(LOCAL_FILE, S3_BUCKET, S3_KEY)
    print("Upload complete.")

def start_glue_job():
    glue = boto3.client("glue", region_name=REGION)
    print("Starting Glue job...")
    response = glue.start_job_run(JobName=GLUE_JOB_NAME)
    job_run_id = response['JobRunId']
    print(f"Glue Job started with Run ID: {job_run_id}")
    return job_run_id

def monitor_glue_job(job_run_id):
    glue = boto3.client("glue", region_name=REGION)
    print("Monitoring Glue job...")

    while True:
        response = glue.get_job_run(
            JobName=GLUE_JOB_NAME,
            RunId=job_run_id
        )
        status = response['JobRun']['JobRunState']
        print(f"Current Status: {status}")

        if status in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
            print(f"Final Status: {status}")
            break

        time.sleep(30)

if __name__ == "__main__":
    try:
        upload_to_s3()
        job_run_id = start_glue_job()
        monitor_glue_job(job_run_id)
    except Exception as e:
        print("Error:", str(e))
        sys.exit(1)
