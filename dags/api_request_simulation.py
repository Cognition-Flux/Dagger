import string
import random
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from airflow import DAG
from airflow.decorators import task, task_group
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Define constants
RATE_LIMIT_DELAY = 0.1  # Delay in seconds between API calls to simulate rate limiting

# Define the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "api_simulation_dag",
    default_args=default_args,
    description="A DAG to simulate API requests with concurrency and rate limiting",
    schedule_interval=None,
    catchup=False,
    max_active_tasks=16,
)


# Task to generate DataFrame with random characters
@task
def generate_dataframe():
    df = pd.DataFrame(
        {
            "random_chars": [
                "".join(random.choices(string.ascii_letters + string.digits, k=10))
                for _ in range(100)
            ]
        }
    )
    return df.to_dict(orient="records")


# Task to simulate API request
@task
def simulate_api_request(row):
    random_chars = row["random_chars"]
    # Simulate API processing time
    time.sleep(RATE_LIMIT_DELAY)
    # Generate random JSON response
    response = {
        "input": random_chars,
        "output": "".join(random.choices(string.ascii_letters + string.digits, k=15)),
    }
    return response


# Task to apply random transformation
@task
def apply_transformation(response):
    # Simulate a transformation by reversing the output string
    transformed_output = response["output"][::-1]
    response["transformed_output"] = transformed_output
    return response


# Task to collect all transformed responses
@task
def collect_responses(responses):
    # Aggregate all responses
    aggregated_data = {"responses": responses, "total_count": len(responses)}
    # Store the aggregated data as a JSON file
    with open("/tmp/aggregated_responses.json", "w") as f:
        json.dump(aggregated_data, f)
    return aggregated_data


# Define the task dependencies
with dag:
    # Generate the initial DataFrame
    rows = generate_dataframe()

    # Process each row individually using dynamic task mapping
    api_responses = simulate_api_request.expand(row=rows)
    transformed_responses = apply_transformation.expand(response=api_responses)

    # Collect all transformed responses
    collect_responses(transformed_responses)
