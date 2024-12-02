# %%
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from pydantic import BaseModel, ValidationError
from datetime import datetime
import logging
import json
from typing import List, Dict, Sequence
from datetime import timedelta

logger = logging.getLogger(__name__)

collected_results_dataset = Dataset("file://collected_results.json")
validated_json_dataset = Dataset("file://validated_data.json")


class TransformedDataModel(BaseModel):
    transformed_number: int


default_args = {
    "owner": "Alejandro",
    "retries": 3,  # Número de reintentos en caso de fallo
    "retry_delay": timedelta(minutes=1),  # Tiempo entre reintentos
}


@dag(
    dag_id="DAG_ramas",
    start_date=datetime(2024, 10, 1, 15, 30),
    schedule_interval="@daily",
    catchup=False,
    tags=["ejemplo-DAG-ramas"],
    default_args=default_args,
)
def combined_pipeline():
    """
    doc
    """

    @task
    def crear_1():
        logger.info("Generando primer conjunto de datos")
        return [{"number": n} for n in range(5)]

    @task
    def crear_2():
        logger.info("Generando segundo conjunto de datos")
        return [{"number": n + 10} for n in range(5)]

    @task
    def transformar_1(data: dict):
        logger.info(f"Procesando flujo 1: {data}")
        return {"transformed_number": data["number"] * 1000}

    @task
    def transformar_2(data: dict):
        logger.info(f"Procesando flujo 2: {data}")
        return {"transformed_number": data["number"] * 2000}

    @task(outlets=[collected_results_dataset])
    def colectar(results_1: Sequence[Dict], results_2: Sequence[Dict]):
        """
        Combina los resultados de ambos flujos.

        Args:
            results_1: Secuencia de resultados del primer flujo (LazyXComSelectSequence)
            results_2: Secuencia de resultados del segundo flujo (LazyXComSelectSequence)
        """
        try:
            logger.info("Iniciando recolección de resultados")

            # Convertimos las secuencias lazy a listas
            results_1_list = list(results_1)
            results_2_list = list(results_2)

            logger.info(f"Resultados flujo 1: {results_1_list}")
            logger.info(f"Resultados flujo 2: {results_2_list}")

            # Combinamos las listas una vez convertidas
            combined_results = results_1_list + results_2_list

            logger.info(f"Total de resultados combinados: {len(combined_results)}")

            try:
                with open("collected_results.json", "w") as f:
                    json.dump(combined_results, f)
                logger.info("Resultados guardados exitosamente en JSON")
            except IOError as e:
                logger.error(f"Error al guardar en JSON: {e}")
                raise

            return combined_results

        except Exception as e:
            logger.error(f"Error en colectar: {str(e)}")
            raise

    @task(inlets=[collected_results_dataset], outlets=[validated_json_dataset])
    def validar(results: List[Dict]):
        validated_results = []
        for data in results:
            try:
                validated = TransformedDataModel(**data)
                validated_results.append(validated.dict())
                logger.info(f"Validado correctamente: {validated}")
            except ValidationError as e:
                logger.error(f"Error de validación: {e}")
        return validated_results

    @task(inlets=[validated_json_dataset])
    def subir(data: List[Dict]):
        for record in data:
            logger.info(f"Subiendo registro: {record}")
        return "Datos cargados exitosamente"

    # Definición del flujo de trabajo
    datasets_1 = crear_1()
    datasets_2 = crear_2()

    results_1 = transformar_1.expand(data=datasets_1)
    results_2 = transformar_2.expand(data=datasets_2)

    collected = colectar(results_1, results_2)
    validated_data = validar(collected)
    subir(validated_data)


dag = combined_pipeline()
