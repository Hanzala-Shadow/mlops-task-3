"""Check the assignment's exact DAG shape with the real Airflow parser."""

from airflow.models import DagBag


def test_three_tasks_and_named_xcom_only():
    bag = DagBag(dag_folder="dags", include_examples=False)
    assert not bag.import_errors, bag.import_errors
    dag = bag.dags.get("currency_exchange_etl")
    assert dag is not None
    assert set(dag.task_ids) == {
        "extract_currency_exchange_rates", "transform_exchange_rate_data", "load_data_into_mongodb"
    }
    assert dag.get_task("extract_currency_exchange_rates").downstream_task_ids == {"transform_exchange_rate_data"}
    assert dag.get_task("transform_exchange_rate_data").downstream_task_ids == {"load_data_into_mongodb"}
    assert not dag.get_task("load_data_into_mongodb").downstream_task_ids
    assert all(not task.do_xcom_push for task in dag.tasks)
    assert dag.catchup is False
    assert dag.max_active_runs == 1
