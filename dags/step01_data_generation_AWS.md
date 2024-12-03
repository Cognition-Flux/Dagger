from airflow.decorators import task, dag  # Decoradores para definir tareas y DAGs
from airflow.providers.amazon.aws.hooks.s3 import (
    S3Hook,
)  # Hook para interactuar con Amazon S3
from airflow.datasets import Dataset  # Clase para definir datasets como dependencias
from datetime import datetime, timedelta
import json
import random  #
import string  #
from pydantic import (
    BaseModel,
    Field,
    ValidationError,
    validator,
)  # Para validación de datos

# Configuración de constantes para Amazon S3
BUCKET_NAME = "etl-demo-alejandro"  # Nombre del bucket S3
REGION_NAME = "us-east-1"  # Región de AWS
S3_KEY_INPUT = "random_data.json"  # Nombre del archivo en S3
AWS_CONN_ID = "aws_default"  # ID de la conexión AWS en Airflow

# Define el dataset de entrada como dependencia
input_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_INPUT}")

# Argumentos por defecto para el DAG
default_args = {
    "owner": "alejandro",  # Propietario del DAG
    "depends_on_past": False,  # No depende de ejecuciones anteriores
    "email_on_retry": False,  # No envía emails en reintentos
    "retries": 9,  # Número de reintentos
    "retry_delay": timedelta(seconds=9),  # Tiempo entre reintentos
}


# Modelo de datos usando Pydantic para validación
class DataModel(BaseModel):
    # Campo ID: debe ser un entero entre 1 y 1000
    id: int = Field(..., ge=1, le=1000, description="ID must be between 1 and 1000")

    # Campo nombre: string entre 1 y 50 caracteres
    name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Name must be 1-50 characters long",
    )

    # Campo valor: float entre 1.0 y 100.0
    value: float = Field(
        ..., ge=1.0, le=100.0, description="Value must be between 1.0 and 100.0"
    )

    # Campo timestamp: fecha y hora
    timestamp: datetime

    # Validador personalizado para el nombre: solo letras permitidas
    @validator("name")
    def name_must_be_letters(cls, v):
        if not v.isalpha():
            raise ValueError("Name must contain only alphabetic characters")
        return v

    # Validador para timestamp: no puede ser futuro
    @validator("timestamp")
    def timestamp_cannot_be_future(cls, v):
        if v > datetime.now():
            raise ValueError("Timestamp cannot be in the future")
        return v

    # Validador para value: debe tener exactamente 2 decimales
    @validator("value")
    def value_must_have_two_decimals(cls, v):
        if round(v, 2) != v:
            raise ValueError("Value must be rounded to two decimal places")
        return v


# Definición del DAG usando el decorador @dag
@dag(
    dag_id="step01_data_generation_AWS",  # Identificador único del DAG
    default_args=default_args,  # Argumentos por defecto definidos anteriormente
    description="Generates data, validates it, and uploads to S3.",  # Descripción del DAG
    start_date=datetime(2024, 10, 1, 15, 30),  # Fecha de inicio
    schedule_interval="@daily",  # Frecuencia de ejecución
    catchup=False,  # No ejecuta DAGs históricos
    tags=["AWS", "s3"],  # Etiquetas para organización
)
def step01_data_generation():
    @task
    def create_or_clear_bucket():
        # Crea o verifica la existencia del bucket S3
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        if not s3_hook.check_for_bucket(bucket_name=BUCKET_NAME):
            s3_hook.create_bucket(bucket_name=BUCKET_NAME, region_name=REGION_NAME)
            print(f"Bucket '{BUCKET_NAME}' created.")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")

    @task
    def generate_data():
        # Genera datos aleatorios
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
        # Valida los datos usando el modelo Pydantic
        try:
            validated_data = DataModel(**data)
            print(f"Validated data: {validated_data}")
            return validated_data.model_dump(mode="json")
        except ValidationError as e:
            print(f"Validation error: {e}")
            raise

    @task(outlets=[input_dataset])
    def upload_data(validated_data):
        # Sube los datos validados a S3
        data_json = json.dumps(validated_data)
        s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
        s3_hook.load_string(
            string_data=data_json,
            key=S3_KEY_INPUT,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        print(f"Data uploaded to s3://{BUCKET_NAME}/{S3_KEY_INPUT}")

    # Define la secuencia de ejecución de las tareas
    create_or_clear_bucket_task = create_or_clear_bucket()
    generated_data_task = generate_data()
    validated_data_task = validate_data(generated_data_task)
    upload_data_task = upload_data(validated_data_task)

    # Establece las dependencias entre tareas usando el operador >>
    (
        create_or_clear_bucket_task
        >> generated_data_task
        >> validated_data_task
        >> upload_data_task
    )


# Instancia el DAG
dag = step01_data_generation()
