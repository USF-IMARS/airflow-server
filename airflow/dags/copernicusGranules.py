from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 5, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'copernicus_granule_download',
    default_args=default_args,
    description='Download granule monthly',
    schedule_interval='@monthly',
    catchup=True,
    max_active_runs=1,
    tags=['backfill']
)


def dl_granule(**kwargs):
    formatted_date = kwargs['ds']
    target_date = datetime.strptime(formatted_date, "%Y-%m-%d")
    month_subset_str = target_date.strftime("*%Y%m*")

    print(f"Processing granule download for date: {formatted_date}")

    import copernicusmarine
    from pprint import pprint

    get_result_monthly = copernicusmarine.get(
        dataset_id=kwargs['collection_name'],
        filter=month_subset_str,
        no_directories=True,
        output_directory="/srv/pgs/copernicus"
    )

    pprint(f"List of saved files: {get_result_monthly}")

    # Fail task if authentication issue arises
    if "Copernicus Marine username" in str(get_result_monthly):
        raise AirflowFailException("Authentication error: Copernicus Marine credentials required.")

    # Skip task if no data to download based on returned message
    if hasattr(get_result_monthly, 'status') and get_result_monthly.status == '003':
        raise AirflowSkipException("No data to download, skipping task.")


# PythonOperator definition
download_task = PythonOperator(
    task_id='download_monthly_granule',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_phy-so_anfc_0.083deg_P1M-m'
    },
    dag=dag,
)
