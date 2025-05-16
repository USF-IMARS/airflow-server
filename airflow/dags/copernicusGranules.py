from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
import glob
import os
import copernicusmarine
from pprint import pprint
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
    'start_date': datetime(2015, 1, 1),
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

    # Check for files in save_dir that match the pattern
    search_pattern = os.path.join(kwargs['save_dir'], month_subset_str)
    existing_files = glob.glob(search_pattern)
    if existing_files:
        print(f"File(s) {existing_files} already exist. Skipping download.")
        raise AirflowSkipException("File already exists, skipping task.")

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

verticalCurrentsTask = PythonOperator(
    task_id='vertical_currents_cmems_phy',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_phy-wcur_anfc_0.083deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/vertical_currents'
    },
    dag=dag,
)

oceanCarbonMYTask = PythonOperator(
    task_id='ocean_carbon_cmems_my',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_obs-mob_glo_bgc-car_my_irr-i',
        'save_dir': '/srv/pgs/copernicus/ocean_carbon_my'
    },
    dag=dag,
)

oceanCarbonNRTTask = PythonOperator(
    task_id='ocean_carbon_cmems_nrt',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_obs-mob_glo_bgc-car_nrt_irr-i',
        'save_dir': '/srv/pgs/copernicus/ocean_carbon_NRT'
    },
    dag=dag,
)

biogeoCarbonTask = PythonOperator(
    task_id='biogeochem_carbon',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-car_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_carbon'
    },
    dag=dag,
)

biogeoCO2Task = PythonOperator(
    task_id='biogeochem_co2',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-co2_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_co2'
    },
    dag=dag,
)

biogeoNutrientsTask = PythonOperator(
    task_id='biogeochem_nutrients',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-nut_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_nutrients'
    },
    dag=dag,
)

biogeoOpticsTask = PythonOperator(
    task_id='biogeochem_optics',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-optics_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_optics'
    },
    dag=dag,
)

biogeoPhytoTask = PythonOperator(
    task_id='biogeochem_phyto',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-pft_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_phyto'
    },
    dag=dag,
)

biogeoPPTask = PythonOperator(
    task_id='biogeochem_PP',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-bio_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_pp'
    },
    dag=dag,
)

biogeoZooTask = PythonOperator(
    task_id='biogeochem_zoo',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_bgc-plankton_anfc_0.25deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/biogeochem_zoo'
    },
    dag=dag,
)

altimeterTask = PythonOperator(
    task_id='altimeter',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_mod_glo_phy_anfc_0.083deg_P1D-m',
        'save_dir': '/srv/pgs/copernicus/altimetry'
    },
    dag=dag,
)

seaSurfaceHeightTask = PythonOperator(
    task_id='SSH',
    python_callable=dl_granule,
    op_kwargs={
        'ds': '{{ ds }}',
        'collection_name': 'cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.125deg_P1M-m',
        'save_dir': '/srv/pgs/copernicus/seaSurfaceHeight'
    },
    dag=dag
)
