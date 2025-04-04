from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
'''
How to add a dataset:
1. add PythonOperator below
2. add cronjob to https://github.com/USF-IMARS/erddap-config/blob/master/rsync-cronjobs.sh
    * this copies files onto dune b/c NFS doesn't play nice
3. set up ERDDAP dataset.xml in https://github.com/USF-IMARS/erddap-config/tree/master/datasets
    * use GenerateDatasets.sh on the ERDDAP server
    * this should trigger a github action to build the full datasets.xml
    * the config should update on dune on the hour
'''



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
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
        output_directory=kwargs['save_dir']
    )

    pprint(f"List of saved files: {get_result_monthly}")

    # Fail task if authentication issue arises
    if "Copernicus Marine username" in str(get_result_monthly):
        raise AirflowFailException("Authentication error: Copernicus Marine credentials required.")

    # Skip task if no data to download based on returned message
    if hasattr(get_result_monthly, 'status') and get_result_monthly.status == '003':
        raise AirflowSkipException("No data to download, skipping task.")


salinityTask = PythonOperator(
    task_id='salinity_cmems_phy',
    python_callable=dl_granule,
    start_date=datetime(2022, 5, 1),  # Overridden start date for windTask
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_phy-so_anfc_0.083deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/salinity'
    },
    dag=dag,
)

windTask = PythonOperator(
    task_id='wind_cmems_phy',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_obs-wind_glo_phy_my_l4_P1M',
        'save_dir': '/srv/pgs/copernicus/wind'
    },
    dag=dag,
)
