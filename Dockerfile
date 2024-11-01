FROM ${AIRFLOW_IMAGE_NAME:-apache/airflow:2.10.2}

WORKDIR /opt/airflow/

COPY requirements.txt /opt/airflow/
RUN pip install -r requirements.txt

COPY ./mining/ /opt/airflow/dags
