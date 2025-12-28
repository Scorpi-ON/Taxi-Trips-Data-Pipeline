from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, cast

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, chain, task

if TYPE_CHECKING:
    import psycopg2

DAG_ID = "nyc_taxi_pipeline"
POSTGRES_CONN_ID = "postgres_default"

RAW_SCHEMA_NAME = "raw"
RAW_TABLE_NAME = "nyc_taxi_trips"
CSV_PATH = Path("/opt/airflow/data/nyc_taxi_raw.csv")
MSC_TZ = timezone(timedelta(hours=3))

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
        print("Rows in CSV:", max(sum(1 for _ in csv_file) - 1, 0))
        csv_file.seek(0)

        cur = cast("psycopg2.extensions.cursor", cur)
        header = "trip_id" + csv_file.readline()
        columns = [f'"{col.strip()}" TEXT' for col in header.split(",")]
        print(columns)
        create_table_sql = f"""
        CREATE TABLE {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (
            {",\n".join(columns)}
        )
        """
        print(create_table_sql)
        cur.execute(create_table_sql)
        conn.commit()

        csv_file.seek(0)

        cur.copy_expert(
            f"""
            COPY {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}
            FROM STDIN WITH (FORMAT csv, HEADER true)
            """,
            csv_file,
        )
        conn.commit()

    loaded_row_count = hook.get_first(
        f"SELECT COUNT(*) FROM {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}",  # noqa: S608
    )[0]

    print("Rows loaded:", loaded_row_count)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=MSC_TZ),
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

    chain(
        drop_old_table,
        create_schema,
        create_and_fill_raw_table(),
    )
