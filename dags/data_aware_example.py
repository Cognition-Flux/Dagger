from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime
import json
import random

# Define the datasets
random_json_dataset = Dataset("file://random.json")
transformed_json_dataset = Dataset("file://transformed.json")
validated_json_dataset = Dataset("file://validated.json")

# First DAG: Produces a random JSON file
with DAG(
    dag_id="ex_data_aware_1_produce_random_json",
    start_date=datetime(2023, 10, 1),
    schedule_interval="@once",  # Runs once or set to any schedule you prefer
    catchup=False,
) as dag1:

    @task(outlets=[random_json_dataset])
    def create_random_json(outlet_events=None):
        data = {"number": random.randint(1, 100)}
        with open("random.json", "w") as f:
            json.dump(data, f)
        print(f"Created random.json with data: {data}")
        # Add extra info to outlet event
        if outlet_events:
            outlet_events[random_json_dataset].extra = {
                "generated_number": data["number"]
            }

    create_random_json()

# Second DAG: Processes the random JSON file when it's updated
with DAG(
    dag_id="ex_data_aware_2_process_random_json",
    start_date=datetime(2023, 10, 1),
    schedule=[random_json_dataset],  # Triggers when random.json is updated
    catchup=False,
) as dag2:

    @task(inlets=[random_json_dataset], outlets=[transformed_json_dataset])
    def transform_json(inlet_events=None, outlet_events=None):
        # Access extra info from inlet events
        if inlet_events:
            events = inlet_events[random_json_dataset]
            last_event = events[-1]
            generated_number = last_event.extra.get("generated_number")
            print(f"Generated number from inlet event: {generated_number}")
        else:
            generated_number = None

        with open("random.json", "r") as f:
            data = json.load(f)
        # Perform a transformation, e.g., multiply the number by 2
        transformed_number = data["number"] * 2
        transformed_data = {"number": transformed_number}
        with open("transformed.json", "w") as f:
            json.dump(transformed_data, f)
        print(f"Transformed data and wrote to transformed.json: {transformed_data}")
        # Add extra info to outlet event
        if outlet_events:
            outlet_events[transformed_json_dataset].extra = {
                "transformed_number": transformed_number
            }

    transform_json()

# Third DAG: Validates the transformed JSON data when it's updated
with DAG(
    dag_id="ex_data_aware_3_validate_transformed_json",
    start_date=datetime(2023, 10, 1),
    schedule=[transformed_json_dataset],  # Triggers when transformed.json is updated
    catchup=False,
) as dag3:

    @task(inlets=[transformed_json_dataset], outlets=[validated_json_dataset])
    def validate_data(inlet_events=None, outlet_events=None):
        # Access extra info from inlet events
        if inlet_events:
            events = inlet_events[transformed_json_dataset]
            last_event = events[-1]
            transformed_number = last_event.extra.get("transformed_number")
            print(f"Transformed number from inlet event: {transformed_number}")
        else:
            transformed_number = None

        # Read transformed.json
        with open("transformed.json", "r") as f:
            data = json.load(f)

        # Perform Pydantic validation
        from pydantic import BaseModel, ValidationError

        class TransformedDataModel(BaseModel):
            number: int

        try:
            validated_data = TransformedDataModel(**data)
            # Write validated.json
            with open("validated.json", "w") as f:
                json.dump(validated_data.dict(), f)
            print(
                f"Validated data and wrote to validated.json: {validated_data.dict()}"
            )
            # Add extra info to outlet event
            if outlet_events:
                outlet_events[validated_json_dataset].extra = {"validation": "passed"}
        except ValidationError as e:
            print("Validation error:", e)
            # Add extra info to outlet event
            if outlet_events:
                outlet_events[validated_json_dataset].extra = {
                    "validation": "failed",
                    "errors": str(e),
                }
            raise

    validate_data()

# Fourth DAG: Mocks uploading the validated JSON data when it's updated
with DAG(
    dag_id="ex_data_aware_4_upload_validated_json",
    start_date=datetime(2023, 10, 1),
    schedule=[validated_json_dataset],  # Triggers when validated.json is updated
    catchup=False,
) as dag4:

    @task(inlets=[validated_json_dataset])
    def upload_data(inlet_events=None):
        # Access extra info from inlet events
        if inlet_events:
            events = inlet_events[validated_json_dataset]
            last_event = events[-1]
            validation_status = last_event.extra.get("validation")
        else:
            validation_status = None

        if validation_status != "passed":
            print("Data validation failed, not uploading.")
            return

        # Read validated.json
        with open("validated.json", "r") as f:
            validated_data = json.load(f)

        # Mock uploading the data
        print(f"Uploading validated data: {validated_data}")

    upload_data()
