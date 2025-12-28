from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING, cast

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import DAG, chain, task

if TYPE_CHECKING:
    import psycopg2

DAG_ID = "nyc_taxi_pipeline"
RAW_SCHEMA_NAME = "raw"
RAW_TABLE_NAME = "nyc_taxi_trips"
CSV_PATH = Path("/opt/airflow/dags/../data/nyc_taxi_raw.csv")
MSC_TZ = timezone(timedelta(hours=3))

POSTGRES_CONN_ID = "postgres_default"

DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _count_csv_rows(path: Path) -> int:
    with path.open("r", encoding="utf-8") as f:
        return max(sum(1 for _ in f) - 1, 0)


@task()
def load_raw() -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    csv_row_count = _count_csv_rows(CSV_PATH)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur = cast("psycopg2.extensions.cursor", cur)
            cur.execute(f"TRUNCATE TABLE {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}")
            with CSV_PATH.open("r", encoding="utf-8") as f:
                cur.copy_expert(
                    f"""
                    COPY {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (
                        trip_id,
                        vendor_id,
                        pickup_datetime,
                        dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        ratecode_id,
                        store_and_fwd_flag,
                        pu_location_id,
                        do_location_id,
                        payment_type,
                        fare_amount,
                        extra,
                        mta_tax,
                        tip_amount,
                        tolls_amount,
                        improvement_surcharge,
                        total_amount
                    )
                    FROM STDIN WITH (FORMAT csv, HEADER true)
                    """,
                    f,
                )
        conn.commit()

    loaded = hook.get_first(
        f"SELECT COUNT(*) FROM {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME}",  # noqa: S608
    )[0]

    print("Rows in CSV:", csv_row_count)
    print("Rows loaded:", loaded)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1, tzinfo=MSC_TZ),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
):
    create_raw_schema = SQLExecuteQueryOperator(
        task_id="create_raw_schema",
        conn_id=POSTGRES_CONN_ID,
        sql=f"CREATE SCHEMA IF NOT EXISTS {RAW_SCHEMA_NAME}",
    )

    create_raw_table = SQLExecuteQueryOperator(
        task_id="create_raw_table",
        conn_id=POSTGRES_CONN_ID,
        sql=f"""
        CREATE TABLE IF NOT EXISTS {RAW_SCHEMA_NAME}.{RAW_TABLE_NAME} (
            trip_id BIGINT PRIMARY KEY,
            vendor_id INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count INTEGER,
            trip_distance NUMERIC,
            ratecode_id INTEGER,
            store_and_fwd_flag TEXT,
            pu_location_id INTEGER,
            do_location_id INTEGER,
            payment_type INTEGER,
            fare_amount NUMERIC,
            extra NUMERIC,
            mta_tax NUMERIC,
            tip_amount NUMERIC,
            tolls_amount NUMERIC,
            improvement_surcharge NUMERIC,
            total_amount NUMERIC
        )
        """,
    )

    chain(
        create_raw_schema,
        create_raw_table,
        load_raw(),
    )
