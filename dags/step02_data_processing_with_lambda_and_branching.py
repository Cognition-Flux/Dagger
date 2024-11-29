# %%
from airflow.decorators import task, dag  # Decoradores para definir tareas y DAGs
from airflow.providers.amazon.aws.hooks.s3 import S3Hook  # Hook para interactuar con S3
from airflow.hooks.base import BaseHook  # Hook base para obtener conexiones
from airflow.datasets import Dataset  # Para definir dependencias basadas en datos
from datetime import datetime, timedelta  #
import json  #
import boto3  # SDK de AWS para Python
from airflow.utils.trigger_rule import TriggerRule  # Reglas de activación de tareas
import random  #
import os  #

# Configuraciones de AWS y S3
BUCKET_NAME = "etl-demo-alejandro"  # Nombre del bucket S3
REGION_NAME = "us-east-1"  # Región de AWS
S3_KEY_INPUT = "random_data.json"  # Ruta del archivo de entrada en S3
S3_KEY_OUTPUT = "transformed_data.json"  # Ruta del archivo de salida en S3
AWS_CONN_ID = "aws_default"  # ID de la conexión AWS en Airflow
LAMBDA_FUNCTION_NAME = "airflow-etl-demo"  # Nombre de la función Lambda

# Definición de datasets para seguimiento de dependencias
input_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_INPUT}")  # Dataset de entrada
output_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")  # Dataset de salida

# Obtención de credenciales AWS desde la conexión de Airflow
aws_conn = BaseHook.get_connection(AWS_CONN_ID)
AWS_ACCESS_KEY_ID = aws_conn.login  # ID de acceso AWS
AWS_SECRET_ACCESS_KEY = aws_conn.password  # Clave secreta AWS

# Variable para modo de prueba
TEST_MODE = False  # Controla la simulación de fallos aleatorios

# Argumentos por defecto para el DAG
default_args = {
    "owner": "alejandro",  # Propietario del DAG
    "depends_on_past": False,  # No depende de ejecuciones anteriores
    "email_on_retry": False,  # No envía emails en reintentos
    "retries": 1,  # Número de reintentos
    "retry_delay": timedelta(seconds=1),  # Tiempo entre reintentos
}


class Notification:
    """Clase para manejar notificaciones escribiendo mensajes en un archivo de texto."""

    def __init__(self, filename="notification.txt"):
        # Obtiene el directorio home de Airflow
        airflow_home = os.getenv("AIRFLOW_HOME", "/usr/local/airflow")
        # Construye la ruta completa del archivo de notificaciones
        self.filepath = os.path.join(airflow_home, filename)

    def send(self, message):
        """Escribe el mensaje de notificación en el archivo."""
        try:
            with open(self.filepath, "a") as file:
                file.write(f"{datetime.now()}: {message}\n")
            print(f"Notification written to {self.filepath}")
        except Exception as e:
            print(f"Failed to write notification: {e}")


# Definición del DAG usando el decorador @dag
@dag(
    dag_id="step02_data_processing",  # ID único del DAG
    default_args=default_args,  # Argumentos por defecto
    description="Processes data using AWS Lambda when new data is available.",  # Descripción
    start_date=datetime(2024, 10, 1, 15, 30),  # Fecha de inicio
    schedule=[input_dataset],  # Se activa cuando input_dataset se actualiza
    catchup=False,  # No ejecuta DAGs históricos
    tags=["AWS", "s3", "lambda", "data-aware"],  # Etiquetas para organización
)
def step02_data_processing():
    def test_failure(probability):
        """Función para simular fallos con una probabilidad dada."""
        if TEST_MODE and random.random() < probability:
            raise Exception("Simulated failure for testing notifications")

    @task(inlets=[input_dataset])
    def download_and_check_data():
        """Descarga datos de S3 y realiza verificaciones de sanidad."""

        # Simula fallo con probabilidad 0.5 si está en modo prueba
        test_failure(0.5)

        # Descarga datos usando S3Hook
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        data_json = s3_hook.read_key(
            key=S3_KEY_INPUT,
            bucket_name=BUCKET_NAME,
        )
        print(f"Downloaded data: {data_json}")

        # Validación de datos
        try:
            data = json.loads(data_json)
            required_keys = {"id", "name", "value", "timestamp"}
            # Verifica que estén todas las claves requeridas
            if not required_keys.issubset(data.keys()):
                missing = required_keys - data.keys()
                raise ValueError(f"Missing keys in data: {missing}")

            # Verifica tipos de datos
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
        """Invoca función Lambda de AWS para transformar datos."""

        # Simula fallo con probabilidad 0.5 si está en modo prueba
        test_failure(0.5)

        try:
            # Inicializa cliente Lambda
            lambda_client = boto3.client(
                "lambda",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME,
            )

            # Prepara input para Lambda
            lambda_input = {"body": json.dumps(data)}

            # Invoca función Lambda
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType="RequestResponse",  # Invocación síncrona
                Payload=json.dumps(lambda_input),
            )

            # Procesa respuesta de Lambda
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
        """Sube datos transformados a S3 y emite evento de dataset."""

        # Simula fallo con probabilidad 0.5 si está en modo prueba
        test_failure(0.5)

        # Verifica que los datos no estén vacíos
        if not data or (isinstance(data, dict) and len(data) == 0):
            raise ValueError(
                "Transformed data is empty or an empty dictionary. Cannot proceed with upload."
            )

        # Serializa datos y sube a S3
        transformed_data_json = json.dumps(data)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=transformed_data_json,
            key=S3_KEY_OUTPUT,
            bucket_name=BUCKET_NAME,
            replace=True,  # Sobrescribe si existe
        )
        print(f"Transformed data uploaded to s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")

    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def send_notification():
        """Envía notificación si alguna tarea upstream falla."""
        notification = Notification()
        message = "A failure has occurred in the pipeline."
        notification.send(message)

    # Definición de tareas y sus dependencias
    download_and_check_data_task = download_and_check_data()
    invoke_lambda_transform_task = invoke_lambda_transform(download_and_check_data_task)
    upload_transformed_data_task = upload_transformed_data(invoke_lambda_transform_task)

    # Configuración de tarea de notificación
    send_notification_task = send_notification()
    send_notification_task << [upload_transformed_data_task]  # Se ejecuta si hay fallo

    # Flujo principal del pipeline
    (
        download_and_check_data_task
        >> invoke_lambda_transform_task
        >> upload_transformed_data_task
    )


dag = step02_data_processing()
