#!/usr/bin/env bash
docker compose down --volumes --rmi all && docker rmi -f "${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.3}" postgres:13 redis:7.2-bookworm

