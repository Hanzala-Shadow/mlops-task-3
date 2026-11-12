"""Trigger two real DAG runs, inspect named XComs, and save run evidence."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

BASE_URL = "http://127.0.0.1:8080/api/v2"
DAG = "currency_exchange_etl"
EVIDENCE = Path(__file__).resolve().parents[1] / "reports" / "evidence"
EVIDENCE.mkdir(parents=True, exist_ok=True)


def api(path, data=None, method=None):
    request = Request(BASE_URL + path, data=json.dumps(data).encode() if data is not None else None,
                      headers={"Content-Type": "application/json"}, method=method)
    with urlopen(request, timeout=20) as response:
        return json.load(response)


api(f"/dags/{DAG}", {"is_paused": False}, "PATCH")
evidence = {"dag_id": DAG, "verified_at": datetime.now(timezone.utc).isoformat(), "runs": []}
for attempt in (1, 2):
    logical_date = datetime.now(timezone.utc).isoformat()
    run_id = f"task3_verification_{attempt}_{int(time.time())}"
    api(f"/dags/{DAG}/dagRuns", {"dag_run_id": run_id, "logical_date": logical_date, "conf": {}}, "POST")
    for _ in range(120):
        run = api(f"/dags/{DAG}/dagRuns/{run_id}")
        if run["state"] in {"success", "failed"}:
            break
        time.sleep(2)
    else:
        raise RuntimeError(f"Timed out waiting for {run_id}")
    if run["state"] != "success":
        raise RuntimeError(f"DAG run failed: {run_id}")
    tasks = api(f"/dags/{DAG}/dagRuns/{run_id}/taskInstances")
    task_rows = [{key: row.get(key) for key in ("task_id", "state", "start_date", "end_date")} for row in tasks["task_instances"]]
    assert len(task_rows) == 3 and all(row["state"] == "success" for row in task_rows)
    xcoms = {}
    for task_id, key in (
        ("extract_currency_exchange_rates", "raw_exchange_rates"),
        ("transform_exchange_rate_data", "transformed_exchange_rates"),
        ("load_data_into_mongodb", "mongodb_load_summary"),
    ):
        xcoms[key] = api(f"/dags/{DAG}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}")
    raw_value = xcoms["raw_exchange_rates"]["value"]
    transformed = xcoms["transformed_exchange_rates"]["value"]
    summary = xcoms["mongodb_load_summary"]["value"]
    assert len(transformed) == len(raw_value["rates"]) == summary["processed"]
    assert all(set(document) == {
        "base_currency", "target_currency", "exchange_rate", "rate_date", "source", "retrieved_at"
    } for document in transformed)
    if attempt == 2:
        previous = evidence["runs"][0]["xcoms"]["transformed_exchange_rates"]["value"]
        identities = lambda rows: {(r["base_currency"], r["target_currency"], r["rate_date"]) for r in rows}
        if identities(previous) == identities(transformed):
            assert summary["upserted"] == 0 and summary["matched"] == len(transformed)
    evidence["runs"].append({"run_id": run_id, "state": run["state"], "tasks": task_rows, "xcoms": xcoms})
    (EVIDENCE / "airflow_runs.json").write_text(json.dumps(evidence, indent=2))
    print(f"Run {attempt}: {run_id} — three tasks succeeded; all three named XComs verified", flush=True)
print(f"Evidence saved to {EVIDENCE / 'airflow_runs.json'}")
