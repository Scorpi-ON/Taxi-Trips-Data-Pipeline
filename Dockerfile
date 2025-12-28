FROM apache/airflow:3.1.5

USER airflow
RUN pip install --no-cache-dir dbt-core==1.11.2 dbt-postgres==1.10.0
