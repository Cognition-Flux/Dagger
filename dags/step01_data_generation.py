from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import json
import random
import string
from pydantic import BaseModel, Field, ValidationError, validator

BUCKET_NAME = "etl-demo-alejandro"
REGION_NAME = "us-east-1"
S3_KEY_INPUT = "random_data.json"
AWS_CONN_ID = "aws_default"

input_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_INPUT}")

default_args = {
    "owner": "alejandro",
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 9,
    "retry_delay": timedelta(seconds=9),
}


class DataModel(BaseModel):
    id: int = Field(..., ge=1, le=1000, description="ID must be between 1 and 1000")
    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Name must be 1-50 characters long",
    )
    value: float = Field(
        ..., ge=1.0, le=100.0, description="Value must be between 1.0 and 100.0"
    )
    timestamp: datetime

    @validator("name")
    def name_must_be_letters(cls, v):
        if not v.isalpha():
            raise ValueError("Name must contain only alphabetic characters")
        return v

    @validator("timestamp")
    def timestamp_cannot_be_future(cls, v):
        if v > datetime.now():
            raise ValueError("Timestamp cannot be in the future")
        return v

    @validator("value")
    def value_must_have_two_decimals(cls, v):
        if round(v, 2) != v:
            raise ValueError("Value must be rounded to two decimal places")
        return v


with DAG(
    dag_id="step01_data_generation",
    default_args=default_args,
    description="Generates data, validates it, and uploads to S3.",
    start_date=datetime.now(),
    schedule_interval="@hourly",
    catchup=False,
    tags=["data-generation", "s3", "dataset"],
) as dag:

    @task
    def create_or_clear_bucket():
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if not s3_hook.check_for_bucket(bucket_name=BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME, region_name=REGION_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")

    @task
    def generate_data():
        data = {
            "id": random.randint(1, 1000),
            "name": "".join(random.choices(string.ascii_letters, k=10)),
            "value": round(random.uniform(1.0, 100.0), 2),
            "timestamp": datetime.now().isoformat(),
        }
        print(f"Generated data: {data}")
        return data

    @task
    def validate_data(data):
        # Validate data using Pydantic
        try:
            validated_data = DataModel(**data)
            print(f"Validated data: {validated_data}")
            # Return a dictionary with serializable data
            return validated_data.model_dump(mode="json")
        except ValidationError as e:
            print(f"Validation error: {e}")
            raise

    @task(outlets=[input_dataset])
    def upload_data(validated_data):
        data_json = json.dumps(validated_data)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=data_json,
            key=S3_KEY_INPUT,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"Data uploaded to s3://{BUCKET_NAME}/{S3_KEY_INPUT}")

    # Define task dependencies
    create_or_clear_bucket_task = create_or_clear_bucket()
    generated_data_task = generate_data()
    validated_data_task = validate_data(generated_data_task)
    upload_data_task = upload_data(validated_data_task)

    (
        create_or_clear_bucket_task
        >> generated_data_task
        >> validated_data_task
        >> upload_data_task
    )
