FROM apache/airflow:2.8.1

USER root   

RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir psycopg2-binary pandas openpyxl