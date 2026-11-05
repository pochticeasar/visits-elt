"""ELT visits_v1: load (TSV → s3://raw) → transform (raw → s3://warehouse).

Оба шага идемпотентны: load пишет те же ключи и удаляет лишние, transform
перезаписывает партиции. Поэтому повтор задачи или запуска безопасен.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="visits_elt",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={"batch": Param("2014-03", type="string"), "src": Param("/data/visits_v1.tsv", type="string")},
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
) as dag:
    load = BashOperator(
        task_id="load",
        bash_command="python3 /opt/jobs/load.py --src {{ params.src }} --batch {{ params.batch }}",
    )
    transform = BashOperator(
        task_id="transform",
        bash_command="spark-submit /opt/jobs/transform.py --batch {{ params.batch }}",
    )
    load >> transform
