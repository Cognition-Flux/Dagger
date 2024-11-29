## Convención básica del DAG

### Cada DAG debe seguir estos patrones generales:

```python
from airflow.datasets import Dataset  # 
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import logging

# variables de AWS
BUCKET_NAME = ""  # Nombre del bucket S3
REGION_NAME = ""  # Región de AWS
S3_KEY_INPUT = ""  # Ruta del archivo de entrada en S3
S3_KEY_OUTPUT = ""  # Ruta del archivo de salida en S3
AWS_CONN_ID = ""  # ID de la conexión AWS en Airflow
LAMBDA_FUNCTION_NAME = ""  # Nombre de la función Lambda
#otras vars de AWS...

# Obtención de credenciales AWS desde la conexión de Airflow
aws_conn = BaseHook.get_connection(AWS_CONN_ID)
AWS_ACCESS_KEY_ID = aws_conn.login  # ID de acceso AWS
AWS_SECRET_ACCESS_KEY = aws_conn.password  # Clave secreta AWS
#otras credenciales...

# Definición de datasets para seguimiento de dependencias
input_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_INPUT}")  # Dataset de entrada
output_dataset = Dataset(f"s3://{BUCKET_NAME}/{S3_KEY_OUTPUT}")  # Dataset de salida
#otros datasets...

# Variable para modo de prueba
TEST_MODE = False  # Controla la simulación de fallos aleatorios

#Otras variables ...

logger = logging.getLogger(__name__)

default_args = {
    "owner": "Alejandro",
    "retries": 3,  # 
    "retry_delay": timedelta(minutes=1),  # 
}


@dag(
    dag_id="nombre_del_dag",
    start_date=datetime(2024, 10, 1, 15, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["ejemplo"],
    default_args=default_args,
)
def nombre_del_dag():
    """
    docs
    """
    @task()
    def tarea_1():
        # Lógica de la tarea
        pass

    @task()
    def tarea_2():
        # Lógica de la tarea
        pass

    # Definición del flujo
    tarea_1() >> tarea_2()

# Instanciación del DAG
dag_instance = nombre_del_dag()
```