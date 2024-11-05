FROM apache/airflow:slim-2.10.2-python3.9

ENV AIRFLOW_HOME=/opt/airflow


USER root
RUN apt-get update && apt-get install -y curl unzip && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf aws awscliv2.zip && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

RUN python3 -m pip install pipx && \
    python3 -m pipx ensurepath && \
    pipx install poetry 
RUN python3 -m pip install pydantic

COPY pyproject.toml poetry.lock /opt/airflow/

WORKDIR /opt/airflow

RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi


USER root

COPY ./config/* /airflow-config/
#COPY ./dags ${AIRFLOW_HOME}/dags
# Accept build arguments
ARG AWS_ACCESS_KEY_ID
ARG AWS_SECRET_ACCESS_KEY
ARG AWS_DEFAULT_REGION

# Set environment variables
ENV AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}
ENV AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}
ENV AWS_DEFAULT_REGION=${AWS_DEFAULT_REGION}

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chmod -R 777 /airflow-config/

USER airflow

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]


