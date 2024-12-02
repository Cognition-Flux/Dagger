#!/usr/bin/env bash
#
set -e
#export AIRFLOW__CORE__EXECUTOR=CeleryExecutor
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=128
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=128
export AIRFLOW__SCHEDULER__MAX_THREADS=128
export AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE=128
export AIRFLOW__SCHEDULER__PARSER_PROCESSING_PLANTS=64
export AIRFLOW__CORE__PARALLELISM=128
export AIRFLOW__CORE__DAG_CONCURRENCY=64
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
export AIRFLOW__CELERY__WORKER_CONCURRENCY=128
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=64
export AIRFLOW__WEBSERVER__WORKERS=64
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=10
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=30
export AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=10
export AIRFLOW__CORE__MIN_SERIALIZED_DAG_FETCH_INTERVAL=10
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
export AIRFLOW__CORE__STORE_SERIALIZED_DAGS=True
export AIRFLOW__CORE__DEFAULT_TIMEZONE=America/Santiago
export AIRFLOW__WEBSERVER__DEFAULT_UI_TIMEZONE=America/Santiago
export AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK=True

airflow db migrate

airflow users create \
          --username dag-test \
          --password dag-test \
          --firstname Ain \
          --lastname Water \
          --role Admin \
          --email alejandro.acevedo@ainwater.com

airflow connections delete 'aws_default' || true

airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-login ${AWS_ACCESS_KEY_ID:-xxx} \
    --conn-password ${AWS_SECRET_ACCESS_KEY:-xxx} \
    --conn-extra "{\"region_name\": \"${AWS_DEFAULT_REGION:-us-east-1}\"}"

airflow pools set extract_pool 8 "pool para tareas de extracción" --include-deferred
airflow pools set transform_pool 8 "pool para tareas de transformación" --include-deferred
airflow pools set load_pool 8 "pool para tareas de carga" --include-deferred

airflow scheduler &
exec airflow webserver


