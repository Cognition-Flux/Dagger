# %%
from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset, DatasetAlias
from datetime import datetime
import json
import random
import logging

# Obtener el logger de Airflow
logger = logging.getLogger(__name__)

json_dataset_1 = Dataset("file://json_dataset_1.json")
json_dataset_2 = Dataset("file://json_dataset_2.json")
transformed_json_dataset = Dataset("file://transformed_json_dataset.json")
validated_json_dataset = Dataset("file://validated_json_dataset.json")


with DAG(
    dag_id="mi_dag_unificado",
    start_date=datetime(2023, 10, 1),
    schedule_interval="@once",
    catchup=False,
) as dag:

    @task(outlets=[json_dataset_1])
    def create_random_json_1(*, outlet_events):
        # Generar un número aleatorio y guardarlo en random_1.json
        data = {"number": random.randint(1, 100)}
        with open("random_1.json", "w") as f:
            json.dump(data, f)
        logger.info(f"Creado random_1.json con data: {data}")

        # Emitir un evento de dataset usando outlet_events
        outlet_events[json_dataset_1].add(extra={"generated_number": data["number"]})
        logger.info(
            f"Evento de salida para random_json_dataset_1: {outlet_events[json_dataset_1]}"
        )

    @task(outlets=[json_dataset_2])
    def create_random_json_2(*, outlet_events):
        # Generar un número aleatorio y guardarlo en random_2.json
        data = {"number": random.randint(1, 100)}
        with open("random_2.json", "w") as f:
            json.dump(data, f)
        logger.info(f"Creado random_2.json con data: {data}")

        # Emitir un evento de dataset usando outlet_events
        outlet_events["random_json_dataset_2"].add(
            Dataset("file://random_2.json"), extra={"generated_number": data["number"]}
        )
        logger.info(
            f"Evento de salida para random_json_dataset_2: {outlet_events['random_json_dataset_2']}"
        )

    @task(
        inlets=[json_dataset_1, json_dataset_2],
        outlets=[transformed_json_dataset],
    )
    def transform_json(*, inlet_events, outlet_events):
        # Acceder a los eventos de entrada
        events_1 = inlet_events[random_json_alias_1]
        events_2 = inlet_events[random_json_alias_2]
        last_event_1 = events_1[-1]
        last_event_2 = events_2[-1]
        generated_number_1 = last_event_1.extra.get("generated_number")
        generated_number_2 = last_event_2.extra.get("generated_number")
        logger.info(
            f"Números recibidos de eventos de entrada: {generated_number_1}, {generated_number_2}"
        )

        # Leer random_1.json y random_2.json y transformar los números
        with open("random_1.json", "r") as f1, open("random_2.json", "r") as f2:
            data1 = json.load(f1)
            data2 = json.load(f2)

        # Sumar los dos números
        transformed_number = data1["number"] + data2["number"]
        transformed_data = {"number": transformed_number}

        # Escribir los datos transformados en transformed.json
        with open("transformed.json", "w") as f:
            json.dump(transformed_data, f)
        logger.info(
            f"Datos transformados y guardados en transformed.json: {transformed_data}"
        )

        # Emitir evento de dataset usando outlet_events
        outlet_events["transformed_json_dataset"].add(
            Dataset("file://transformed.json"),
            extra={"transformed_number": transformed_number},
        )
        logger.info(
            f"Evento de salida para transformed_json_dataset: {outlet_events['transformed_json_dataset']}"
        )

    @task(inlets=[transformed_json_dataset], outlets=[validated_json_dataset])
    def validate_data(*, inlet_events, outlet_events):
        # Acceder a los eventos de entrada
        events = inlet_events[transformed_json_alias]
        last_event = events[-1]
        transformed_number = last_event.extra.get("transformed_number")
        logger.info(
            f"Número transformado recibido de eventos de entrada: {transformed_number}"
        )

        # Leer transformed.json y validar los datos
        with open("transformed.json", "r") as f:
            data = json.load(f)

        # Validar los datos usando Pydantic
        from pydantic import BaseModel, ValidationError

        class TransformedDataModel(BaseModel):
            number: int

        try:
            validated_data = TransformedDataModel(**data)

            # Escribir los datos validados en validated.json
            with open("validated.json", "w") as f:
                json.dump(validated_data.dict(), f)
            logger.info(
                f"Datos validados y escritos en validated.json: {validated_data.dict()}"
            )

            # Emitir evento de dataset usando outlet_events
            outlet_events["validated_json_dataset"].add(
                Dataset("file://validated.json"), extra={"validation": "passed"}
            )
            logger.info(
                f"Evento de salida para validated_json_dataset: {outlet_events['validated_json_dataset']}"
            )
        except ValidationError as e:
            logger.error("Error de validación.", exc_info=True)

            # Emitir evento de dataset con información de error
            outlet_events["validated_json_dataset"].add(
                Dataset("file://validated.json"),
                extra={"validation": "failed", "errors": str(e)},
            )
            logger.info(
                f"Evento de salida para validated_json_dataset: {outlet_events['validated_json_dataset']}"
            )
            raise

    @task(inlets=[validated_json_dataset])
    def upload_data(*, inlet_events):
        # Acceder a los eventos de entrada
        events = inlet_events[validated_json_alias]
        last_event = events[-1]
        validation_status = last_event.extra.get("validation")
        logger.info(
            f"Estatus de validación recibido de eventos de entrada: {validation_status}"
        )

        # No proceder si la validación falló
        if validation_status != "passed":
            logger.warning("La validación falló, los datos no serán subidos.")
            return

        # Leer validated.json y simular la subida de los datos
        with open("validated.json", "r") as f:
            validated_data = json.load(f)

        # Simular la subida de los datos validados
        logger.info(f"Subiendo datos validados: {validated_data}")

    # Definir la secuencia de tareas
    random_task_1 = create_random_json_1()
    random_task_2 = create_random_json_2()
    transform_task = transform_json()
    validate_task = validate_data()
    upload_task = upload_data()

    # Establecer dependencias
    [random_task_1, random_task_2] >> transform_task >> validate_task >> upload_task
