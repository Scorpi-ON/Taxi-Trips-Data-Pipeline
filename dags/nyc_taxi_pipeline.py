import os
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, chain, task

if TYPE_CHECKING:
    import psycopg2.extensions

DAG_ID = "nyc_taxi_pipeline"
POSTGRES_CONN_ID = "postgres_default"

RAW_SCHEMA_NAME = "raw"
RAW_TABLE_NAME = "nyc_taxi_trips"
CSV_PATH = Path("/opt/airflow/data/nyc_taxi_raw.csv")
DBT_PATH = os.environ.get("DBT_PROFILES_DIR")

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@task()
def create_and_fill_raw_table() -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    with (
        hook.get_conn() as conn,
        conn.cursor() as cur,
        CSV_PATH.open("r", encoding="utf-8") as csv_file,
    ):
        cur = cast("psycopg2.extensions.cursor", cur)
        header = "trip_id" + csv_file.readline()
        columns = [f'"{col.strip()}" TEXT' for col in header.split(",")]

        create_table_sql = f"""
        CREATE TABLE {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (
            {",\n".join(columns)}
        )
        """
        cur.execute(create_table_sql)
        conn.commit()

        cur.copy_expert(
            f"""
            COPY {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
            FROM STDIN WITH (FORMAT csv, HEADER false)
            """,
            csv_file,
        )
        conn.commit()

        csv_file.seek(0)
        csv_row_count = max(sum(1 for _ in csv_file) - 1, 0)

    loaded_row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}",  # noqa: S608
    )[0]

    print("Rows loaded:", loaded_row_count)
    print("Rows in CSV:", csv_row_count)


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
):
    drop_old_table = SQLExecuteQueryOperator(
        task_id="drop_old_table",
        conn_id=POSTGRES_CONN_ID,
        sql=f"DROP TABLE IF EXISTS {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} CASCADE",
    )

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id=POSTGRES_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA_NAME}",
    )

    run_staging = BashOperator(
        task_id="run_staging",
        cwd=DBT_PATH,
        bash_command="dbt run --select stg_nyc_taxi_trips",
    )

    test_staging = BashOperator(
        task_id="test_staging",
        cwd=DBT_PATH,
        bash_command="dbt test --select stg_nyc_taxi_trips",
    )

    run_marts = BashOperator(
        task_id="run_marts",
        cwd=DBT_PATH,
        bash_command="dbt run --select mart_taxi_daily",
    )

    test_marts = BashOperator(
        task_id="test_marts",
        cwd=DBT_PATH,
        bash_command="dbt test --select mart_taxi_daily",
    )

    chain(
        drop_old_table,
        create_schema,
        create_and_fill_raw_table(),
        run_staging,
        test_staging,
        run_marts,
        test_marts,
    )
