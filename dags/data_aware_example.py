from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime
import json
import random

random_json_dataset = Dataset("file://random.json")
transformed_json_dataset = Dataset("file://transformed.json")
validated_json_dataset = Dataset("file://validated.json")

# Primer DAG: Produce un archivo JSON con un número aleatorio
with (
    DAG(
        dag_id="ex_data_aware_1_produce_random_json",
        start_date=datetime(2023, 10, 1),
        schedule_interval="@once",  # Se ejecuta una sola vez o según el intervalo configurado
        catchup=False,
    ) as dag1
):

    @task(outlets=[random_json_dataset])
    def create_random_json(outlet_events=None):
        # Genera un número aleatorio y lo guarda en un archivo JSON
        data = {"number": random.randint(1, 100)}
        with open("random.json", "w") as f:
            json.dump(data, f)
        print(f"Creado random.json con datos: {data}")

        # Opcional: agregar información extra al evento de salida
        if outlet_events:
            outlet_events[random_json_dataset].extra = {
                "generated_number": data["number"]
            }

    create_random_json()

# Segundo DAG: Procesa el archivo JSON cuando se actualiza
with DAG(
    dag_id="ex_data_aware_2_process_random_json",
    start_date=datetime(2023, 10, 1),
    schedule=[random_json_dataset],  # Se activa cuando random.json se actualiza
    catchup=False,
) as dag2:

    @task(inlets=[random_json_dataset], outlets=[transformed_json_dataset])
    def transform_json(inlet_events=None, outlet_events=None):
        # Accede a los datos adicionales del evento de entrada
        if inlet_events:
            events = inlet_events[random_json_dataset]
            last_event = events[-1]
            generated_number = last_event.extra.get("generated_number")
            print(f"Número generado desde el evento de entrada: {generated_number}")
        else:
            generated_number = None

        # Lee el archivo random.json y transforma el número
        with open("random.json", "r") as f:
            data = json.load(f)
        transformed_number = data["number"] * 2
        transformed_data = {"number": transformed_number}

        # Escribe los datos transformados en transformed.json
        with open("transformed.json", "w") as f:
            json.dump(transformed_data, f)
        print(
            f"Transformados los datos y guardados en transformed.json: {transformed_data}"
        )

        # Agrega información extra al evento de salida
        if outlet_events:
            outlet_events[transformed_json_dataset].extra = {
                "transformed_number": transformed_number
            }

    transform_json()

# Tercer DAG: Valida los datos transformados
with DAG(
    dag_id="ex_data_aware_3_validate_transformed_json",
    start_date=datetime(2023, 10, 1),
    schedule=[
        transformed_json_dataset
    ],  # Se activa cuando transformed.json se actualiza
    catchup=False,
) as dag3:

    @task(inlets=[transformed_json_dataset], outlets=[validated_json_dataset])
    def validate_data(inlet_events=None, outlet_events=None):
        # Accede a los datos adicionales del evento de entrada
        if inlet_events:
            events = inlet_events[transformed_json_dataset]
            last_event = events[-1]
            transformed_number = last_event.extra.get("transformed_number")
            print(
                f"Número transformado desde el evento de entrada: {transformed_number}"
            )
        else:
            transformed_number = None

        # Lee el archivo transformed.json
        with open("transformed.json", "r") as f:
            data = json.load(f)

        # Valida los datos utilizando Pydantic
        from pydantic import BaseModel, ValidationError

        class TransformedDataModel(BaseModel):
            number: int

        try:
            validated_data = TransformedDataModel(**data)

            # Escribe los datos validados en validated.json
            with open("validated.json", "w") as f:
                json.dump(validated_data.dict(), f)
            print(f"Validado y escrito en validated.json: {validated_data.dict()}")

            # Agrega información extra al evento de salida
            if outlet_events:
                outlet_events[validated_json_dataset].extra = {"validation": "passed"}
        except ValidationError as e:
            print("Error de validación:", e)

            # Agrega información extra al evento de salida en caso de error
            if outlet_events:
                outlet_events[validated_json_dataset].extra = {
                    "validation": "failed",
                    "errors": str(e),
                }
            raise

    validate_data()

# Cuarto DAG: Simula la subida de los datos validados
with DAG(
    dag_id="ex_data_aware_4_upload_validated_json",
    start_date=datetime(2023, 10, 1),
    schedule=[validated_json_dataset],  # Se activa cuando validated.json se actualiza
    catchup=False,
) as dag4:

    @task(inlets=[validated_json_dataset])
    def upload_data(inlet_events=None):
        # Accede a los datos adicionales del evento de entrada
        if inlet_events:
            events = inlet_events[validated_json_dataset]
            last_event = events[-1]
            validation_status = last_event.extra.get("validation")
        else:
            validation_status = None

        # Si la validación falla, no se realiza la subida
        if validation_status != "passed":
            print("La validación falló, no se suben los datos.")
            return

        # Lee el archivo validated.json
        with open("validated.json", "r") as f:
            validated_data = json.load(f)

        # Simula la subida de los datos validados
        print(f"Subiendo datos validados: {validated_data}")

    upload_data()
