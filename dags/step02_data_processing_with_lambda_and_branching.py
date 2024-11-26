# %%
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import json
import boto3
from airflow.utils.trigger_rule import TriggerRule
import random  # Import random module
import os

#######################
# AWS and S3 configurations
BUCKET_NAME = "etl-demo-alejandro"
REGION_NAME = "us-east-1"
S3_KEY_INPUT = "random_data.json"
S3_KEY_OUTPUT = "transformed_data.json"
AWS_CONN_ID = "aws_default"
LAMBDA_FUNCTION_NAME = "airflow-etl-demo"

# Define datasets
input_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_INPUT}")
output_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")

# Retrieve AWS credentials from Airflow connection
aws_conn = BaseHook.get_connection(AWS_CONN_ID)
AWS_ACCESS_KEY_ID = aws_conn.login
AWS_SECRET_ACCESS_KEY = aws_conn.password

# Test mode variable
TEST_MODE = False  # Set to True to enable random failures, False to disable

default_args = {
    "owner": "alejandro",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=1),
}


class Notification:
    """Class to handle notifications by writing messages to a text file."""

    def __init__(self, filename="notification.txt"):
        # Determine Airflow's home directory
        airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
        # Set the full path for the notification file
        self.filepath = os.path.join(airflow_home, filename)

    def send(self, message):
        """Write the notification message to the file."""
        try:
            with open(self.filepath, "a") as file:
                file.write(f"{datetime.now()}: {message}\n")
            print(f"Notification written to {self.filepath}")
        except Exception as e:
            print(f"Failed to write notification: {e}")


with DAG(
    dag_id="step02_data_processing",
    default_args=default_args,
    description="Processes data using AWS Lambda when new data is available.",
    start_date=datetime(2023, 10, 1),  # Use a static start_date
    schedule=[input_dataset],  # Triggers when input_dataset is updated
    catchup=False,
    tags=["data-processing", "s3", "dataset", "lambda"],
) as dag:

    def test_failure(probability):
        """Function to simulate failure with given probability."""
        if TEST_MODE and random.random() < probability:
            raise Exception("Simulated failure for testing notifications")

    @task(inlets=[input_dataset])
    def download_and_check_data():
        """Download data from S3 and perform sanity checks."""

        # Test variable to trigger notification with probability 0.4
        test_failure(0.5)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        data_json = s3_hook.read_key(
            key=S3_KEY_INPUT,
            bucket_name=BUCKET_NAME,
        )
        print(f"Downloaded data: {data_json}")
        try:
            data = json.loads(data_json)
            required_keys = {"id", "name", "value", "timestamp"}
            if not required_keys.issubset(data.keys()):
                missing = required_keys - data.keys()
                raise ValueError(f"Missing keys in data: {missing}")
            assert isinstance(data["id"], int), "id must be an integer"
            assert isinstance(data["name"], str), "name must be a string"
            assert isinstance(data["value"], float), "value must be a float"
            assert isinstance(data["timestamp"], str), "timestamp must be a string"
            print("Sanity check passed: Required keys and data types are correct.")
        except (json.JSONDecodeError, AssertionError, ValueError) as e:
            raise ValueError(f"Data sanity check failed: {e}")
        return data

    @task
    def invoke_lambda_transform(data):
        """Invoke AWS Lambda function to transform data."""

        # Test variable to trigger notification with probability 0.4
        test_failure(0.5)

        try:
            lambda_client = boto3.client(
                "lambda",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME,
            )

            lambda_input = {"body": json.dumps(data)}

            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType="RequestResponse",  # Synchronous invocation
                Payload=json.dumps(lambda_input),
            )

            response_payload = response["Payload"].read().decode("utf-8")
            response_data = json.loads(response_payload)

            transformed_data = json.loads(response_data.get("body", "{}"))
            print(f"Transformed data from Lambda: {transformed_data}")

            return transformed_data

        except Exception as e:
            print(f"Error invoking Lambda: {e}")
            raise

    @task(outlets=[output_dataset])
    def upload_transformed_data(data):
        """Upload transformed data to S3 and emit dataset event."""

        # Test variable to trigger notification with probability 0.4
        test_failure(0.5)

        # Check if data is empty or an empty dictionary
        if not data or (isinstance(data, dict) and len(data) == 0):
            raise ValueError(
                "Transformed data is empty or an empty dictionary. Cannot proceed with upload."
            )

        transformed_data_json = json.dumps(data)

        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=transformed_data_json,
            key=S3_KEY_OUTPUT,
            bucket_name=BUCKET_NAME,
            replace=True,  # Overwrite if it already exists
        )
        print(f"Transformed data uploaded to s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def send_notification():
        """Send notification if any upstream task fails."""
        notification = Notification()
        message = "A failure has occurred in the pipeline."
        notification.send(message)

    # Define task dependencies
    download_and_check_data_task = download_and_check_data()
    invoke_lambda_transform_task = invoke_lambda_transform(download_and_check_data_task)
    upload_transformed_data_task = upload_transformed_data(invoke_lambda_transform_task)

    # Notification task dependencies
    send_notification_task = send_notification()
    send_notification_task << [
        upload_transformed_data_task,
    ]

    # Main linear flow
    (
        download_and_check_data_task
        >> invoke_lambda_transform_task
        >> upload_transformed_data_task
    )
