from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pydantic import BaseModel, ValidationError
from datetime import datetime, timedelta

# from datetime import datetime
import logging
import json
import traceback
from typing import List, Dict, Sequence
from pathlib import Path

# Configuración mejorada del sistema de logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger.setLevel(logging.INFO)

# Definición de rutas de archivos para mejor gestión
DATA_DIR = Path("./data")
DATA_DIR.mkdir(exist_ok=True)

# Datasets compartidos que actúan como mecanismo de coordinación entre DAGs
collected_results_dataset = Dataset("file://collected_results.json")
validated_json_dataset = Dataset("file://validated_data.json")


class ProcessingError(Exception):
    """Excepción personalizada para errores de procesamiento"""

    pass


class TransformedDataModel(BaseModel):
    """
    Modelo Pydantic para validación de datos transformados.
    Define la estructura esperada de los datos después de la transformación.
    """

    transformed_number: int

    class Config:
        """Configuración adicional del modelo para mejor validación"""

        extra = "forbid"  # Previene campos adicionales no esperados
        validate_assignment = True  # Valida los datos incluso después de la creación


default_args = {
    "owner": "Alejandro",
    "retries": 3,  # Número de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=1),  # Tiempo entre reintentos
}


@dag(
    dag_id="data_aware_1_crear_transformar_colectar",
    start_date=datetime(2024, 10, 1, 15, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["ejemplo-data-aware"],
    default_args=default_args,
)
def dynamic_process_pipeline():
    """
    Primer DAG del flujo data-aware.
    Responsable de crear, transformar y colectar datos iniciales.
    Este DAG actúa como productor de datos para el segundo DAG.
    """

    @task
    def create_random_jsons() -> List[Dict[str, int]]:
        """
        Genera un conjunto de datos de ejemplo para procesamiento.

        Returns:
            List[Dict[str, int]]: Lista de diccionarios con números para procesar
        """
        try:
            logger.info("Iniciando generación de datos aleatorios")
            data = [{"number": n} for n in range(5)]
            logger.info(f"Generados {len(data)} registros exitosamente")
            return data
        except Exception as e:
            logger.error(
                f"Error generando datos: {str(e)}\nStack trace: {traceback.format_exc()}"
            )
            raise ProcessingError("Error en generación de datos")

    @task
    def transform_json(data: dict) -> Dict[str, int]:
        """
        Transforma un único registro de datos.
        Esta tarea será expandida para procesamiento paralelo.

        Args:
            data: Diccionario conteniendo un número para transformar

        Returns:
            Dict[str, int]: Diccionario con el número transformado
        """
        try:
            logger.info(f"Iniciando transformación de datos: {data}")
            if not isinstance(data.get("number"), (int, float)):
                raise ValueError(f"Dato inválido: {data}")

            result = {"transformed_number": data["number"] * 1000}
            logger.info(f"Transformación exitosa: {data} -> {result}")
            return result
        except Exception as e:
            logger.error(
                f"Error en transformación: {str(e)}\nDatos: {data}\nStack trace: {traceback.format_exc()}"
            )
            raise

    @task(outlets=[collected_results_dataset])
    def collect_results(results: Sequence[Dict]) -> List[Dict[str, int]]:
        """
        Colecta y persiste los resultados de las transformaciones paralelas.

        Args:
            results: Secuencia de resultados transformados (LazyXComSelectSequence)

        Returns:
            List[Dict[str, int]]: Lista combinada de resultados
        """
        try:
            logger.info("Iniciando recolección de resultados")

            # Materialización de resultados lazy
            results_list = list(results)
            logger.info(
                f"Resultados materializados exitosamente: {len(results_list)} elementos"
            )

            # Validación básica de resultados
            if not results_list:
                raise ProcessingError("No se encontraron resultados para colectar")

            # Persistencia de resultados
            output_path = DATA_DIR / "collected_results.json"
            try:
                with open(output_path, "w") as f:
                    json.dump(results_list, f)
                logger.info(f"Resultados guardados exitosamente en: {output_path}")
                return results_list
            except IOError as e:
                logger.error(f"Error guardando resultados: {str(e)}")
                raise ProcessingError(f"Error de IO: {str(e)}")

        except Exception as e:
            logger.error(
                f"Error en recolección: {str(e)}\nStack trace: {traceback.format_exc()}"
            )
            raise

    # Definición del flujo de trabajo
    logger.info("Iniciando pipeline de procesamiento dinámico")
    datasets = create_random_jsons()
    results = transform_json.expand(data=datasets)
    collect_results(results)


@dag(
    dag_id="data_aware_2_validar_subir",
    start_date=datetime(2023, 10, 1),
    schedule=[collected_results_dataset],  # Se activa cuando el primer DAG completa
    catchup=False,
    tags=["ejemplo-data-aware"],
    default_args=default_args,
)
def validate_and_upload_pipeline():
    """
    Segundo DAG del flujo data-aware.
    Responsable de validar y cargar los datos procesados por el primer DAG.
    Se activa automáticamente cuando el primer DAG completa y genera el dataset.
    """

    @task(inlets=[collected_results_dataset], outlets=[validated_json_dataset])
    def validate_data() -> List[Dict[str, int]]:
        """
        Valida los datos usando el modelo Pydantic.
        Lee los datos del archivo generado por el primer DAG.

        Returns:
            List[Dict[str, int]]: Lista de datos validados
        """
        try:
            logger.info("Iniciando validación de datos")
            input_path = DATA_DIR / "collected_results.json"

            # Lectura de datos
            try:
                with open(input_path, "r") as f:
                    results = json.load(f)
                logger.info(f"Datos cargados exitosamente: {len(results)} registros")
            except IOError as e:
                logger.error(f"Error leyendo archivo de datos: {str(e)}")
                raise ProcessingError(f"Error de IO: {str(e)}")

            # Validación de datos
            validated_results = []
            validation_errors = []

            for idx, data in enumerate(results):
                try:
                    validated = TransformedDataModel(**data)
                    validated_results.append(validated.dict())
                    logger.debug(f"Registro {idx} validado exitosamente: {validated}")
                except ValidationError as e:
                    error_msg = f"Error validando registro {idx}: {data}"
                    logger.error(f"{error_msg}\nError: {str(e)}")
                    validation_errors.append(
                        {"index": idx, "data": data, "error": str(e)}
                    )

            if validation_errors:
                logger.warning(
                    f"Proceso completado con {len(validation_errors)} errores de validación"
                )
                logger.debug(f"Detalles de errores: {validation_errors}")
            else:
                logger.info("Todos los registros validados exitosamente")

            return validated_results

        except Exception as e:
            logger.error(
                f"Error en validación: {str(e)}\nStack trace: {traceback.format_exc()}"
            )
            raise

    @task(inlets=[validated_json_dataset])
    def upload(data: List[Dict[str, int]]) -> str:
        """
        Simula la carga de datos validados a un sistema externo.

        Args:
            data: Lista de datos validados para cargar

        Returns:
            str: Mensaje de confirmación
        """
        try:
            logger.info(f"Iniciando carga de {len(data)} registros")

            for idx, record in enumerate(data):
                try:
                    # Aquí iría la lógica real de carga
                    logger.info(f"Simulando carga de registro {idx}: {record}")
                except Exception as e:
                    logger.error(f"Error cargando registro {idx}: {str(e)}")
                    raise

            logger.info("Proceso de carga completado exitosamente")
            return "Datos cargados exitosamente"

        except Exception as e:
            logger.error(
                f"Error en proceso de carga: {str(e)}\nStack trace: {traceback.format_exc()}"
            )
            raise

    # Definición del flujo de trabajo
    logger.info("Iniciando pipeline de validación y carga")
    validated_data = validate_data()
    upload(validated_data)


# Instanciación de ambos DAGs
dag1 = dynamic_process_pipeline()
dag2 = validate_and_upload_pipeline()
