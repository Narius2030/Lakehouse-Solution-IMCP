FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt update && \
    apt-get -y install libpq-dev gcc && \
    apt-get update && apt-get install ffmpeg libsm6 libxext6  -y && \
    apt-get clean;

USER airflow

COPY ./requirements.txt .

RUN pip install --no-cache-dir -r ./requirements.txt