"""
Example Airflow DAG to demonstrate calling download_granule.py
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException

# Import the updated function from our script
from copernicus_to_erddap.download_granule import download_granule
from openeo.rest import OpenEoApiError  # Import the specific exception

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2005, 1, 1),  # Starting in 2005
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'copernicus_granule_download',
    default_args=default_args,
    description='Download Sentinel-2 granule monthly starting from 2005',
    schedule_interval='@monthly',  # Runs once a month
    catchup=True,                  # Enable backfilling since 2005
)

def dl_granule(ds, **kwargs):
    """
    Task to download the Sentinel-2 granule for a specific month.
    If no data is available (as indicated by an OpenEoApiError),
    skip the task.
    
    Parameters:
        ds (str): The execution date as a string (format 'YYYY-MM-DD').
    """
    formatted_date = ds  # 'ds' is already in 'YYYY-MM-DD' format
    
    try:
        # Call our function with the date
        output_file = download_granule(formatted_date)
    except OpenEoApiError as e:
        # Only skip if the error message indicates that no data is available
        if "NoDataAvailable" in str(e):
            raise AirflowSkipException(
                f"Skipping download for {formatted_date}: {e}"
            )
        else:
            raise
    
    return f"Successfully downloaded granule to {output_file}"

download_task = PythonOperator(
    task_id='download_monthly_granule',
    python_callable=dl_granule,
    op_kwargs={'ds': '{{ ds }}'},  # Pass the execution date explicitly
    dag=dag,
)

if __name__ == "__main__":
    dag.cli()
