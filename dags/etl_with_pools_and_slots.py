# %%
from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import time

# Definimos el DAG
with DAG(
    dag_id="etl_with_pools_and_slots",
    start_date=datetime(2024, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=10,
) as dag:
    # Tarea inicial que genera muchos JSONs
    @task
    def generate_json_data():
        # Genera una lista de 10 JSONs
        json_list = [{"id": i, "data": [i, i + 1, i + 2]} for i in range(12)]
        print(f"Generados {len(json_list)} JSONs")
        time.sleep(2)
        return json_list

    # Tarea que procesa cada JSON individualmente
    @task(pool="extract_pool", pool_slots=4)
    def extract_json(json_data):
        # Simula el procesamiento de datos
        print(f"Procesando JSON con id: {json_data['id']}")
        time.sleep(1)
        processed_data = {
            "id": json_data["id"],
            "processed_data": [x * 2 for x in json_data["data"]],
        }
        return processed_data

    # Tarea de transformación que procesa cada 'processed_data' individualmente
    @task(pool="transform_pool", pool_slots=2)
    def transform_json(processed_data):
        # Procesa individualmente cada 'processed_data'
        print(f"Transformando datos del JSON con id: {processed_data['id']}")
        transformed = [x + 1 for x in processed_data["processed_data"]]
        transformed_data = {"id": processed_data["id"], "transformed_data": transformed}
        time.sleep(1)
        return transformed_data

    # Tarea de carga que procesa cada 'transformed_data' individualmente
    @task(pool="load_pool", pool_slots=1)
    def load_json(transformed_data):
        # Simula la carga de un dato transformado
        print(
            f"Cargando datos transformados para id {transformed_data['id']}: {transformed_data['transformed_data']}"
        )
        time.sleep(1)

    # Generamos los JSONs
    json_data_list = generate_json_data()

    # Procesamos los JSONs utilizando mapeo
    processed_data = extract_json.expand(json_data=json_data_list)

    # Transformamos los datos procesados utilizando mapeo
    transformed_data = transform_json.expand(processed_data=processed_data)

    # Cargamos los datos transformados utilizando mapeo
    load_json.expand(transformed_data=transformed_data)
