"""Daily Frankfurter -> named XCom -> MongoDB ETL with exactly three tasks."""

from datetime import datetime, timedelta, timezone

from airflow.sdk import dag, get_current_context, task

from dags.currency_etl.pipeline import (
    LOAD_XCOM_KEY,
    RAW_XCOM_KEY,
    TRANSFORMED_XCOM_KEY,
    CurrencyConfig,
    extract_rates,
    load_rates,
    transform_rates,
)


@dag(
    dag_id="currency_exchange_etl",
    description="Daily Frankfurter reference exchange rates stored in MongoDB",
    schedule="0 18 * * *",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "hanzala",
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
        "execution_timeout": timedelta(minutes=5),
    },
    tags=["task-3", "etl", "frankfurter", "mongodb"],
)
def currency_exchange_etl():
    @task(task_id="extract_currency_exchange_rates", do_xcom_push=False)
    def extract():
        ti = get_current_context()["ti"]
        ti.xcom_push(key=RAW_XCOM_KEY, value=extract_rates(CurrencyConfig.from_env()))

    @task(task_id="transform_exchange_rate_data", do_xcom_push=False)
    def transform():
        ti = get_current_context()["ti"]
        raw = ti.xcom_pull(task_ids="extract_currency_exchange_rates", key=RAW_XCOM_KEY)
        documents = transform_rates(raw, CurrencyConfig.from_env())
        ti.xcom_push(key=TRANSFORMED_XCOM_KEY, value=documents)

    @task(task_id="load_data_into_mongodb", do_xcom_push=False)
    def load():
        ti = get_current_context()["ti"]
        documents = ti.xcom_pull(task_ids="transform_exchange_rate_data", key=TRANSFORMED_XCOM_KEY)
        ti.xcom_push(key=LOAD_XCOM_KEY, value=load_rates(documents))

    extract() >> transform() >> load()


currency_exchange_etl()
