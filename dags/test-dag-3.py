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
