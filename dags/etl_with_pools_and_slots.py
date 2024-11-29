# %%
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import time
import logging
import traceback
from typing import List, Dict, Any
import json

# Configuración mejorada del sistema de logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger.setLevel(logging.INFO)


class ETLProcessingError(Exception):
    """Excepción personalizada para errores en el proceso ETL"""

    pass


# Argumentos por defecto para el DAG
default_args = {
    "owner": "Alejandro",
    "retries": 3,  # Número de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=1),  # Tiempo entre reintentos
    "email_on_failure": True,  # Enviar email en caso de fallo
    "email_on_retry": True,  # Enviar email en caso de reintento
}

# Definición del DAG con parámetros específicos para control de concurrencia
with DAG(
    dag_id="etl_with_pools_and_slots",
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=10,  # Limita el número máximo de ejecuciones concurrentes del DAG
    default_args=default_args,
    doc_md="""
    # DAG ETL con Pools y Slots
    
    Este DAG implementa un proceso ETL utilizando pools y slots para control de concurrencia.
    
    ## Estructura
    - Generate: Genera datos de prueba (12 JSONs)
    - Extract: Procesa JSONs (4 slots)
    - Transform: Transforma datos (2 slots)
    - Load: Carga final (1 slot)
    
    ## Pools
    - extract_pool: Para tareas de extracción
    - transform_pool: Para tareas de transformación
    - load_pool: Para tareas de carga
    """,
) as dag:

    @task
    def generate_json_data() -> List[Dict[str, Any]]:
        """
        Genera un conjunto de datos JSON de prueba.

        Returns:
            List[Dict[str, Any]]: Lista de diccionarios con datos de prueba
        """
        try:
            logger.info("Iniciando generación de datos JSON")

            # Genera una lista de 12 JSONs con datos de prueba
            json_list = [{"id": i, "data": [i, i + 1, i + 2]} for i in range(12)]

            logger.info(f"Generados exitosamente {len(json_list)} JSONs")

            # Simulamos algún procesamiento
            time.sleep(2)

            return json_list

        except Exception as e:
            logger.error(
                f"Error en generación de datos: {str(e)}\nStack trace: {traceback.format_exc()}"
            )
            raise ETLProcessingError("Error en generación de datos")

    @task(pool="extract_pool", pool_slots=4)
    def extract_json(json_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Procesa un JSON individual en el pool de extracción.
        Utiliza 4 slots para permitir procesamiento paralelo limitado.

        Args:
            json_data: Diccionario con datos a procesar

        Returns:
            Dict[str, Any]: Diccionario con datos procesados
        """
        try:
            logger.info(f"Iniciando extracción para JSON con id: {json_data['id']}")

            # Validación básica de datos de entrada
            if not isinstance(json_data.get("data"), list):
                raise ValueError(f"Formato inválido para id {json_data['id']}")

            # Simulamos procesamiento
            time.sleep(1)

            # Procesamos los datos
            processed_data = {
                "id": json_data["id"],
                "processed_data": [x * 2 for x in json_data["data"]],
            }

            logger.info(f"Extracción completada para id: {json_data['id']}")
            return processed_data

        except Exception as e:
            logger.error(
                f"Error en extracción para id {json_data.get('id', 'unknown')}: {str(e)}\n"
                f"Datos: {json_data}\nStack trace: {traceback.format_exc()}"
            )
            raise

    @task(pool="transform_pool", pool_slots=2)
    def transform_json(processed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transforma datos procesados en el pool de transformación.
        Utiliza 2 slots para un procesamiento más restrictivo.

        Args:
            processed_data: Diccionario con datos procesados

        Returns:
            Dict[str, Any]: Diccionario con datos transformados
        """
        try:
            logger.info(f"Iniciando transformación para id: {processed_data['id']}")

            # Validación de datos de entrada
            if not isinstance(processed_data.get("processed_data"), list):
                raise ValueError(
                    f"Datos procesados inválidos para id {processed_data['id']}"
                )

            # Transformación de datos
            transformed = [x + 1 for x in processed_data["processed_data"]]
            transformed_data = {
                "id": processed_data["id"],
                "transformed_data": transformed,
            }

            # Simulamos procesamiento
            time.sleep(1)

            logger.info(f"Transformación completada para id: {processed_data['id']}")
            return transformed_data

        except Exception as e:
            logger.error(
                f"Error en transformación para id {processed_data.get('id', 'unknown')}: {str(e)}\n"
                f"Datos: {processed_data}\nStack trace: {traceback.format_exc()}"
            )
            raise

    @task(pool="load_pool", pool_slots=1)
    def load_json(transformed_data: Dict[str, Any]) -> None:
        """
        Carga datos transformados en el pool de carga.
        Utiliza 1 slot para garantizar carga secuencial.

        Args:
            transformed_data: Diccionario con datos transformados
        """
        try:
            logger.info(f"Iniciando carga para id: {transformed_data['id']}")

            # Validación de datos transformados
            if not isinstance(transformed_data.get("transformed_data"), list):
                raise ValueError(
                    f"Datos transformados inválidos para id {transformed_data['id']}"
                )

            # Simulamos la carga de datos
            logger.info(
                f"Cargando datos para id {transformed_data['id']}: "
                f"{transformed_data['transformed_data']}"
            )

            time.sleep(1)
            logger.info(
                f"Carga completada exitosamente para id: {transformed_data['id']}"
            )

        except Exception as e:
            logger.error(
                f"Error en carga para id {transformed_data.get('id', 'unknown')}: {str(e)}\n"
                f"Datos: {transformed_data}\nStack trace: {traceback.format_exc()}"
            )
            raise

    try:
        logger.info("Iniciando pipeline ETL")

        # Generamos los datos iniciales
        json_data_list = generate_json_data()

        # Procesamiento paralelo con control de concurrencia mediante pools
        processed_data = extract_json.expand(json_data=json_data_list)
        transformed_data = transform_json.expand(processed_data=processed_data)
        load_json.expand(transformed_data=transformed_data)

        logger.info("Pipeline ETL configurado exitosamente")

    except Exception as e:
        logger.error(
            f"Error en configuración del pipeline: {str(e)}\nStack trace: {traceback.format_exc()}"
        )
        raise
