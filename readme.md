## DAGs de ejemplo
### **Nombre**: `DAG_ramas`
- **Programación**: Ejecución diaria (@daily)
- **Fecha inicio**: 1 octubre 2024
- **No catchup**: Evita ejecuciones históricas
- **Reintentos**: 3 intentos con delay de 1 minuto entre ellos

### Flujo de Trabajo
1. **Generación de Datos** (Paralelo)
   - `crear_1()`: Genera lista de 5 números [0-4]
   - `crear_2()`: Genera lista de 5 números [10-14]

2. **Transformación** (Paralelo)
   - `transformar_1()`: Multiplica cada número por 1000
   - `transformar_2()`: Multiplica cada número por 2000

3. **Procesamiento de Resultados** (Secuencial)
   - `colectar()`: 
     - Combina resultados de ambas transformaciones
     - Guarda en collected_results.json
     - Utiliza Dataset para trackear el archivo

   - `validar()`:
     - Valida datos contra TransformedDataModel
     - Lee desde collected_results.json
     - Guarda en validated_data.json

   - `subir()`:
     - Procesa los datos validados
     - Lee desde validated_data.json

### Atributos 
- Logging extensivo en cada tarea
- Manejo de errores con try/except
- Validación de datos con Pydantic
- Tracking de dependencias con Datasets

### **Sistema de DAGs Data-Aware**
### Configuración General
- **Sistema de Logging**: Configuración mejorada con formato timestamp
- **Directorio de Datos**: Creación automática de './data'
- **Datasets de Coordinación**:
  - collected_results_dataset
  - validated_json_dataset
  - 
#### DAG 1: `data_aware_1_crear_transformar_colectar`
- **Programación**: Ejecución diaria (@daily)
- **Fecha inicio**: 1 octubre 2024
- **No catchup**: Evita ejecuciones históricas
- **Reintentos**: 3 intentos con delay de 1 minuto

### Flujo de Trabajo DAG 1
1. **Generación** (`create_random_jsons`)
   - Genera lista de 5 números [0-4]
   - Manejo de errores con ProcessingError

2. **Transformación** (`transform_json`)
   - Multiplica cada número por 1000
   - Validación de tipos de datos
   - Procesamiento paralelo con expand

3. **Recolección** (`collect_results`)
   - Materializa resultados lazy
   - Guarda en collected_results.json
   - Activa Dataset para DAG 2

#### DAG 2: `data_aware_2_validar_subir`
- **Programación**: Activado por Dataset (collected_results_dataset)
- **Fecha inicio**: 1 octubre 2023
- **No catchup**: Evita ejecuciones históricas
- **Reintentos**: Misma configuración que DAG 1

### Flujo de Trabajo DAG 2
1. **Validación** (`validate_data`)
   - Lee collected_results.json
   - Valida contra TransformedDataModel
   - Registra errores de validación
   - Genera validated_json_dataset

2. **Carga** (`upload`)
   - Procesa registros validados
   - Simula carga a sistema externo
   - Logging detallado por registro

### Características
- Manejo extensivo de errores con try/except
- Logging detallado en cada operación
- Modelo Pydantic con validación estricta
- Coordinación entre DAGs mediante Datasets
- Procesamiento paralelo en transformaciones
- Trazabilidad completa de operaciones


### **Nombre**: `etl_with_pools_and_slots`
- **Programación**: Ejecución diaria (@daily)
- **Fecha inicio**: 1 octubre 2024
- **No catchup**: Evita ejecuciones históricas
- **Max active runs**: 10 ejecuciones concurrentes máximo
- **Reintentos**: 3 intentos con delay de 1 minuto
- **Notificaciones**: Emails en fallos y reintentos

### Pools y Slots
1. **Extract Pool**: 4 slots
   - Permite procesamiento paralelo limitado
   - Procesa JSONs en paralelo

2. **Transform Pool**: 2 slots
   - Procesamiento más restrictivo
   - Transforma datos en paralelo limitado

3. **Load Pool**: 1 slot
   - Garantiza carga secuencial
   - Evita conflictos de escritura

### Flujo de Trabajo
1. **Generación** (`generate_json_data`)
   - Crea 12 JSONs de prueba
   - Formato: `{"id": n, "data": [n, n+1, n+2]}`
   - Sin restricción de pool

2. **Extracción** (`extract_json`)
   - Pool: extract_pool (4 slots)
   - Duplica valores del array de entrada
   - Procesamiento paralelo de 4 JSONs a la vez

3. **Transformación** (`transform_json`)
   - Pool: transform_pool (2 slots)
   - Suma 1 a cada valor del array
   - Procesa 2 registros simultáneamente

4. **Carga** (`load_json`)
   - Pool: load_pool (1 slot)
   - Simula carga secuencial
   - Un registro a la vez

### Características
- Logging extensivo con timestamps
- Manejo de errores personalizado (ETLProcessingError)
- Control granular de concurrencia
- Validaciones en cada etapa
- Simulación de procesamiento con delays
- Trazabilidad completa del proceso
- Documentación integrada en DocMD

### **Nombre**: `step01_data_generation`
- **Programación**: Ejecución diaria (@daily)
- **Fecha inicio**: 1 octubre 2024, 15:30
- **No catchup**: Evita ejecuciones históricas
- **Tags**: AWS, s3
- **Reintentos**: 9 intentos con delay de 9 segundos
- **Conexión AWS**: aws_default

### Configuración S3
- **Bucket**: etl-demo-alejandro
- **Región**: us-east-1
- **Archivo**: random_data.json
- **Dataset**: Definido como dependencia para otros DAGs

### Modelo de Datos (Pydantic)
- **id**: Entero (1-1000)
- **name**: String (1-50 caracteres, solo letras)
- **value**: Float (1.0-100.0, 2 decimales)
- **timestamp**: Datetime (no futuro)

### Flujo de Trabajo
1. **Preparación S3** (`create_or_clear_bucket`)
   - Verifica existencia del bucket
   - Crea bucket si no existe

2. **Generación** (`generate_data`)
   - ID aleatorio (1-1000)
   - Nombre aleatorio (10 letras)
   - Valor aleatorio (1.0-100.0)
   - Timestamp actual

3. **Validación** (`validate_data`)
   - Valida contra DataModel
   - Comprueba restricciones personalizadas
   - Convierte a formato JSON

4. **Carga** (`upload_data`)
   - Sube JSON a S3
   - Reemplaza archivo si existe
   - Marca dataset como actualizado

### Características
- Validación estricta con Pydantic
- Validadores personalizados
- Integración con AWS S3
- Dataset como dependencia
- Manejo de errores de validación
- Logs detallados del proceso


### **Nombre**: `step02_data_processing`
- **Programación**: Activado por input_dataset
- **Fecha inicio**: 1 octubre 2024, 15:30
- **No catchup**: Evita ejecuciones históricas
- **Tags**: AWS, s3, lambda, data-aware
- **Reintentos**: 1 intento con delay de 1 segundo

### Configuración AWS
- **S3 Bucket**: etl-demo-alejandro
- **Región**: us-east-1
- **Input**: random_data.json
- **Output**: transformed_data.json 
- **Lambda**: airflow-etl-demo
- **Conexión**: aws_default

### Datasets
- **Input**: s3://etl-demo-alejandro/random_data.json
- **Output**: s3://etl-demo-alejandro/transformed_data.json

### Flujo de Trabajo
1. **Descarga y Validación** (`download_and_check_data`)
  - Descarga datos de S3
  - Valida estructura JSON
  - Verifica tipos de datos
  - Comprueba claves requeridas
  
2. **Transformación Lambda** (`invoke_lambda_transform`) 
  - Invoca función Lambda de AWS
  - Modo síncrono (RequestResponse)
  - Procesa respuesta
  - Valida datos transformados

3. **Carga** (`upload_transformed_data`)
  - Verifica datos no vacíos
  - Sube a S3 como JSON
  - Marca dataset como actualizado

4. **Notificaciones** (`send_notification`)
  - Se activa si falla alguna tarea
  - Escribe en archivo notification.txt
  - Usa TriggerRule.ONE_FAILED

### Características
- Modo prueba para simular fallos
- Sistema de notificaciones
- Validaciones estrictas
- Integración con AWS Lambda
- Manejo de credenciales seguro
- Datasets como dependencias
- Logging detallado

### **Nombre**: `step03_data_consumption_with_lambda`
- **Programación**: Activado por output_dataset
- **Fecha inicio**: 1 octubre 2024, 15:30 
- **No catchup**: Evita ejecuciones históricas
- **Tags**: AWS, s3, data-aware, lambda
- **Reintentos**: 1 intento con delay de 1 minuto

### Configuración AWS
- **S3 Bucket**: etl-demo-alejandro
- **Región**: us-east-1
- **Archivo Entrada**: transformed_data.json
- **Lambda**: airflow-etl-demo-consumption
- **Conexión**: aws_default

### Dataset Dependencia
- **Input**: s3://etl-demo-alejandro/transformed_data.json
 - Actúa como trigger del DAG
 - Se monitorean eventos del dataset

### Flujo de Trabajo
1. **Verificación** (`invoke_lambda_consumption`)
  - Verifica eventos nuevos del dataset
  - Solo procede si hay actualizaciones

2. **Invocación Lambda**
  - Prepara payload con datos del bucket
  - Invoca función de forma síncrona
  - Espera respuesta (RequestResponse)

3. **Procesamiento Respuesta**
  - Verifica código de estado 200
  - Procesa payload de respuesta
  - Maneja errores si falló

### Características
- Integración con AWS Lambda
- Manejo seguro de credenciales
- Dataset como disparador
- Logging de respuestas
- Control de errores
- Modo síncrono de ejecución

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
        return resultado

    @task()
    def tarea_2():
        # Lógica de la tarea
        pass

    # Definición del flujo
    resultado = tarea_1()
    tarea_2(resultado)
# Instanciación del DAG
dag_instance = nombre_del_dag()
```
## Testear DAGs en el contenedor
Para ejecutar comandos dentro del contenedor se dispone del script airflow.sh.  
### Ejemplo: Testear el DAG 'etl_with_pools_and_slots':
```bash
#otorgamos permisos
chmod +x airflow.sh
#abrimos una terminal de bash en el contenedor
./airflow.sh bash
```
Dentro del contenedor:
```bash
#Enlistamos todos los DAGs para verificar que está disponible
airflow dags list
#Testeamos
airflow dags test etl_with_pools_and_slots 2024-01-01
```
También se puede testear cada tarea:
```bash
#Enlistamos las tareas
airflow tasks list etl_with_pools_and_slots
#Ejecutamos una tarea específica. El testeo tiene que ser consistente con las dependencias PARA COMENZAR A TESTEAR.
airflow tasks test etl_with_pools_and_slots generate_json_data 2024-01-01
```