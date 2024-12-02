FROM apache/airflow:2.10.3

ENV AIRFLOW_HOME=/opt/airflow

USER root


COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_HOME}
#RUN chmod -R 777 /airflow-config/

COPY ./dags ${AIRFLOW_HOME}/dags

USER airflow

WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]


