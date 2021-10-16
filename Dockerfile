FROM apache/airflow:2.0.1
USER root
RUN apt-get update 
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt