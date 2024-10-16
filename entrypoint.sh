#!/usr/bin/env bash

set -e

configure_aws_and_conn() {
    echo "Configuring AWS CLI with provided credentials..."
    aws configure set aws_access_key_id "${AWS_ACCESS_KEY_ID}"
    aws configure set aws_secret_access_key "${AWS_SECRET_ACCESS_KEY}"
    aws configure set default.region "${AWS_DEFAULT_REGION:-us-east-1}"

    echo "Adding aws_default connection in Airflow with provided credentials..."
    airflow connections delete 'aws_default' || true
    airflow connections add 'aws_default' \
        --conn-type 'aws' \
        --conn-login "${AWS_ACCESS_KEY_ID}" \
        --conn-password "${AWS_SECRET_ACCESS_KEY}" \
        --conn-extra "{\"region_name\": \"${AWS_DEFAULT_REGION:-us-east-1}\"}"
}


sync_refresh_dags() {
    while true; do
        echo "Syncing DAGs from S3..."
        aws s3 sync s3://etl-airflow-alejandro/dags ${AIRFLOW_HOME}/dags
        echo "Reserializing DAGs..."
        airflow dags reserialize
        echo "Sleeping..."
        sleep 120  # Sleep
    done
}

airflow db init

if ! airflow users list | grep -q "${AIRFLOW_USER_USERNAME:-admin}"; then
    airflow users create \
        --username "${AIRFLOW_USER_USERNAME}" \
        --firstname "${AIRFLOW_USER_FIRSTNAME}" \
        --lastname "${AIRFLOW_USER_LASTNAME}" \
        --role "${AIRFLOW_USER_ROLE}" \
        --email "${AIRFLOW_USER_EMAIL}" \
        --password "${AIRFLOW_USER_PASSWORD}"
fi

if [ "${RUNNING_IN_AWS_ECS}" = "False" ]; then
    configure_aws_and_conn
fi

sync_refresh_dags &

airflow scheduler &

exec airflow webserver


