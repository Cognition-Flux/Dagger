#!/usr/bin/env bash
#
set -e
export AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE=64
export AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW=64
export AIRFLOW__SCHEDULER__MAX_THREADS=64
export AIRFLOW__SCHEDULER__MAX_DAGRUNS_PER_LOOP_TO_SCHEDULE=128
export AIRFLOW__SCHEDULER__PARSER_PROCESSING_PLANTS=64
export AIRFLOW__CORE__PARALLELISM=128
export AIRFLOW__CORE__DAG_CONCURRENCY=64
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
export AIRFLOW__CELERY__WORKER_CONCURRENCY=128
export AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=64
export AIRFLOW__WEBSERVER__WORKERS=8
export AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=10
export AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=100
export AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL=10
export AIRFLOW__CORE__MIN_SERIALIZED_DAG_FETCH_INTERVAL=10

airflow db migrate

airflow users create \
          --username alejandro \
          --password alejandro \
          --firstname FIRST_NAME \
          --lastname LAST_NAME \
          --role Admin \
          --email admin@example.org

airflow scheduler &

exec airflow webserver


