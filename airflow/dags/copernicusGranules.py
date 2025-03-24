"""
Example Airflow DAG to demonstrate calling download_granule.py
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Import the function from our script
from copernicus_to_erddap import download_granule

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'copernicus_granule_download',
    default_args=default_args,
    description='Download Sentinel-2 granule daily',
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def dl_granule(**kwargs):
    """
    Task to download Sentinel-2 granule for a specific date
    """
    # Get execution date from Airflow context
    execution_date = kwargs.get('execution_date')
    
    # Format date for our function
    formatted_date = execution_date.strftime('%Y-%m-%d')
    
    # Call our function with the date
    output_file = download_granule(formatted_date)
    
    return f"Successfully downloaded granule to {output_file}"

# Create task
download_task = PythonOperator(
    task_id='download_daily_granule',
    python_callable=dl_granule,
    provide_context=True,
    dag=dag,
)

# You can add more tasks here that depend on the downloaded file
# For example, processing or notification tasks

if __name__ == "__main__":
    dag.cli()
