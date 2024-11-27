from airflow import DAG
from airflow.decorators import task
from airflow.datasets import Dataset
from datetime import datetime
import json
import logging

# Obtener el logger de Airflow
logger = logging.getLogger(__name__)

random_json_dataset = Dataset("file://random.json")
transformed_json_dataset = Dataset("file://transformed.json")
validated_json_dataset = Dataset("file://validated.json")

# Primer DAG: Produce un archivo JSON con un número aleatorio
with DAG(
    dag_id="ex_data_aware_1_produce_random_json",
    start_date=datetime(2023, 10, 1),
    schedule_interval="@once",  # Se ejecuta una vez o según el intervalo configurado
    catchup=False,
) as dag1:

    @task(outlets=[random_json_dataset])
    def create_random_json(*, outlet_events):
        data = {"number": 777}
        with open("random.json", "w") as f:
            json.dump(data, f)
        logger.info(f"Creado random.json con data: {data}")

        # Añadir información al evento de salida
        outlet_events["create_random_json"] = {"generated_number": data["number"]}
        logger.info(f"Eventos de salida: {outlet_events}")

    create_random_json()

# Segundo DAG: Procesa el archivo JSON cuando se actualiza
with DAG(
    dag_id="ex_data_aware_2_process_random_json",
    start_date=datetime(2023, 10, 1),
    schedule=[random_json_dataset],  # Se activa cuando random.json se actualiza
    catchup=False,
) as dag2:

    @task(inlets=[random_json_dataset], outlets=[transformed_json_dataset])
    def transform_json(*, inlet_events, outlet_events):
        # Acceder a los eventos de entrada
        generated_number = inlet_events["create_random_json"]["generated_number"]
        logger.info(f"El número generado en create_random_json es: {generated_number}")

        # Transformar los datos
        transformed_data = {"number": generated_number * 1000}
        with open("transformed.json", "w") as f:
            json.dump(transformed_data, f)
        logger.info(f"Creado transformed.json con data: {transformed_data}")

        # Añadir información al evento de salida
        outlet_events["transform_json"] = {
            "transformed_number": transformed_data["number"]
        }
        logger.info(f"Eventos de salida: {outlet_events}")

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
    def validate_data(*, inlet_events, outlet_events):
        # Acceder a los eventos de entrada
        events = inlet_events[transformed_json_dataset]
        last_event = events[-1]
        transformed_number = last_event.extra.get("transformed_number")
        logger.info(
            f"Número transformado recibido de eventos de entrada: {transformed_number}"
        )

        # Leer transformed.json
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

            # Añadir información adicional al evento de salida
            outlet_events[validated_json_dataset].add(
                Dataset("file://validated.json"), extra={"validation": "passed"}
            )
            logger.info(f"Eventos de salida: {outlet_events}")
        except ValidationError as e:
            logger.error("Error de validación.", exc_info=True)

            # Añadir información de error al evento de salida
            outlet_events[validated_json_dataset].add(
                Dataset("file://validated.json"),
                extra={"validation": "failed", "errors": str(e)},
            )
            logger.info(f"Eventos de salida: {outlet_events}")
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
    def upload_data(*, inlet_events):
        # Acceder a los eventos de entrada
        events = inlet_events[validated_json_dataset]
        last_event = events[-1]
        validation_status = last_event.extra.get("validation")
        logger.info(
            f"Estatus de validación recibido de eventos de entrada: {validation_status}"
        )

        # No proceder si la validación falló
        if validation_status != "passed":
            logger.warning("La validación falló, los datos no serán subidos.")
            return

        # Leer validated.json
        with open("validated.json", "r") as f:
            validated_data = json.load(f)

        # Simular la subida de los datos validados
        logger.info(f"Subiendo datos validados: {validated_data}")

    upload_data()
