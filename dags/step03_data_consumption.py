# %%
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import json
import boto3

BUCKET_NAME = "etl-demo-alejandro"
S3_KEY_OUTPUT = "transformed_data.json"
AWS_CONN_ID = "aws_default"
LAMBDA_FUNCTION_NAME = "airflow-etl-demo-consumption"
REGION_NAME = "us-east-1"

output_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")

default_args = {
    "owner": "Alejandro",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Retrieve AWS credentials from Airflow connection
aws_conn = BaseHook.get_connection(AWS_CONN_ID)
AWS_ACCESS_KEY_ID = aws_conn.login
AWS_SECRET_ACCESS_KEY = aws_conn.password

with DAG(
    dag_id="step03_data_consumption_with_lambda",
    default_args=default_args,
    description="Consumes transformed data using AWS Lambda.",
    start_date=datetime.now(),
    schedule=[output_dataset],  # Triggers when output_dataset is updated
    catchup=False,
    tags=["data-consumption", "s3", "dataset", "lambda"],
) as dag:

    @task(inlets=[output_dataset])
    def invoke_lambda_consumption(*, inlet_events):
        """Invoke AWS Lambda function to consume data."""
        # Access dataset event metadata
        events = inlet_events[output_dataset]
        if events:
            # Prepare parameters for Lambda
            bucket_name = BUCKET_NAME
            key = S3_KEY_OUTPUT

            # Initialize Lambda client
            lambda_client = boto3.client(
                "lambda",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME,
            )

            # Prepare the event payload
            lambda_event = {
                "bucket_name": bucket_name,
                "key": key,
            }

            # Invoke the Lambda function
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType="RequestResponse",
                Payload=json.dumps(lambda_event),
            )

            response_payload = response["Payload"].read().decode("utf-8")
            response_data = json.loads(response_payload)

            if response_data.get("statusCode") == 200:
                print("Lambda consumption successful.")
                print(f"Response: {response_data['body']}")
            else:
                print(f"Lambda consumption failed: {response_data['body']}")
                raise Exception("Lambda consumption failed.")
        else:
            print("No dataset events found.")

    invoke_lambda_consumption_task = invoke_lambda_consumption()
