FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.2}

WORKDIR /opt/airflow/

COPY pyproject.toml README.md /opt/airflow/

ENV AIRFLOW_CONN_MYSQL_MINING=mysql://airflow_user:airflow_pass@mysql:3306/mining?serverTimezone=UTC
