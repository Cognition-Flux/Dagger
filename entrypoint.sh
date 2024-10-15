#!/usr/bin/env bash

set -e

# aws configure set aws_access_key_id ${AWS_ACCESS_KEY_ID}
# aws configure set aws_secret_access_key ${AWS_SECRET_ACCESS_KEY}
# aws configure set default.region ${AWS_DEFAULT_REGION}

refresh_dags() {
    while true; do
        echo "Syncing DAGs from S3..."
        aws s3 sync s3://etl-airflow-alejandro/dags ${AIRFLOW_HOME}/dags
        echo "Reserializing DAGs..."
        airflow dags reserialize
        echo "Sleeping..."
        sleep 600  # Sleep
    done
}
aws s3 sync s3://etl-airflow-alejandro/dags ${AIRFLOW_HOME}/dags

airflow db init

airflow dags reserialize


if ! airflow users list | grep -q "${AIRFLOW_USER_USERNAME:-admin}"; then
    airflow users create \
        --username "${AIRFLOW_USER_USERNAME}" \
        --firstname "${AIRFLOW_USER_FIRSTNAME}" \
        --lastname "${AIRFLOW_USER_LASTNAME}" \
        --role "${AIRFLOW_USER_ROLE}" \
        --email "${AIRFLOW_USER_EMAIL}" \
        --password "${AIRFLOW_USER_PASSWORD}"
fi

airflow connections delete 'aws_default' || true

airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-login "${AWS_ACCESS_KEY_ID:-none}" \
    --conn-password "${AWS_SECRET_ACCESS_KEY:-none}" \
    --conn-extra "{\"region_name\": \"${AWS_DEFAULT_REGION:-us-east-1}\"}"

refresh_dags &

airflow scheduler &

exec airflow webserver


