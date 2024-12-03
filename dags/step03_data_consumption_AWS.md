from airflow.decorators import task, dag  # Decoradores para definir tareas y DAGs
from airflow.datasets import Dataset  # Para manejar dependencias basadas en datos
from airflow.hooks.base import (
    BaseHook,
)  # Para obtener conexiones configuradas en Airflow
from datetime import datetime, timedelta
import json
import boto3  # SDK de AWS para Python

# Configuraciones y constantes
BUCKET_NAME = "etl-demo-alejandro"  # Nombre del bucket S3 donde se almacenan los datos
S3_KEY_OUTPUT = "transformed_data.json"  # Ruta del archivo de datos transformados en S3
AWS_CONN_ID = "aws_default"  # Identificador de la conexión AWS en Airflow
LAMBDA_FUNCTION_NAME = (
    "airflow-etl-demo-consumption"  # Nombre de la función Lambda a invocar
)
REGION_NAME = "us-east-1"  # Región de AWS donde se encuentran los recursos

# Definición del dataset de salida para seguimiento de dependencias
output_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")

# Argumentos por defecto para el DAG
default_args = {
    "owner": "Alejandro",  # Propietario del DAG
    "depends_on_past": False,  # No depende de ejecuciones anteriores
    "email_on_retry": False,  # No envía emails en reintentos
    "retries": 1,  # Número de reintentos si falla
    "retry_delay": timedelta(minutes=1),  # Espera 1 minuto entre reintentos
}

# Obtención de credenciales AWS desde la conexión configurada en Airflow
aws_conn = BaseHook.get_connection(AWS_CONN_ID)
AWS_ACCESS_KEY_ID = aws_conn.login  # ID de acceso AWS
AWS_SECRET_ACCESS_KEY = aws_conn.password  # Clave secreta AWS


# Definición del DAG usando el decorador @dag
@dag(
    dag_id="step03_data_consumption_AWS",  # Identificador único del DAG
    default_args=default_args,  # Argumentos por defecto definidos anteriormente
    description="Consumes transformed data using AWS Lambda.",  # Descripción del propósito
    start_date=datetime(2024, 10, 1, 15, 30),  # Fecha de inicio del DAG
    schedule=[output_dataset],  # Se ejecuta cuando el dataset de salida se actualiza
    catchup=False,  # No ejecuta DAGs históricos
    tags=["AWS", "s3", "data-aware", "lambda"],  # Etiquetas para organización
)
def step03_data_consumption_with_lambda():
    @task(inlets=[output_dataset])  # Define tarea con dataset de entrada
    def invoke_lambda_consumption(*, inlet_events):
        """Invoca función Lambda de AWS para consumir datos transformados."""

        # Verifica eventos del dataset
        events = inlet_events[output_dataset]
        if events:  # Si hay eventos nuevos
            # Prepara parámetros para Lambda
            bucket_name = BUCKET_NAME
            key = S3_KEY_OUTPUT

            # Inicializa cliente Lambda con credenciales
            lambda_client = boto3.client(
                "lambda",
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                region_name=REGION_NAME,
            )

            # Prepara el payload para la función Lambda
            lambda_event = {
                "bucket_name": bucket_name,
                "key": key,
            }

            # Invoca la función Lambda de forma síncrona
            response = lambda_client.invoke(
                FunctionName=LAMBDA_FUNCTION_NAME,
                InvocationType="RequestResponse",  # Espera respuesta
                Payload=json.dumps(lambda_event),  # Convierte payload a JSON
            )

            # Procesa la respuesta de Lambda
            response_payload = response["Payload"].read().decode("utf-8")
            response_data = json.loads(response_payload)

            # Verifica si la ejecución fue exitosa
            if response_data.get("statusCode") == 200:
                print("Lambda consumption successful.")
                print(f"Response: {response_data['body']}")
            else:
                print(f"Lambda consumption failed: {response_data['body']}")
                raise Exception("Lambda consumption failed.")
        else:
            print("No dataset events found.")  # No hay nuevos eventos para procesar

    # Ejecuta la tarea de invocación Lambda
    invoke_lambda_consumption()


# Instancia el DAG
dag = step03_data_consumption_with_lambda()
