FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.2}

WORKDIR /opt/airflow/

COPY pyproject.toml /opt/airflow/
RUN pip install --upgrade build

ENV AIRFLOW_CONN_MYSQL_MINING mysql://airflow_user:airflow_pass@mysql:3306/mining
ENV AIRFLOW_CONN_REDIS redis://redis:6379/?__extra__=%7B%22db%22%3A+0%2C+%22ssl%22%3A+false%2C+%22ssl_cert_reqs%22%3A+%22none%22%2C+%22ssl_check_hostname%22%3A+false%7D