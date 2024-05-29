"""
backup of homes directory from yin to tpa pgs
"""
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

with DAG(
    'homes_backup',
    catchup=False,  # latest only
    schedule_interval="0 6 * * 6",  # 6am every Sat
    max_active_runs=1,
    default_args={
        "start_date": datetime(2024, 1, 1)
    },
) as dag:
    BashOperator(
        task_id="homes_backup_to_pgs",
        bash_command=(
            "rsync -habuP /srv/imars-objects/homes/ /srv/imars-objects/tpa_pgs/yin/homes/"
        )
    )
    # TODO: add another BashOperator for secondary backup
