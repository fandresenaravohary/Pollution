from airflow import DAG
from airflow.operators.python import PythonOperator  # Correction de l'importation
from airflow.utils.dates import days_ago

from extract1 import first_extract
from transform import transform_data
from load import save_cleaned_data

default_args = {  # Correction du nom de la clé
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'pollution',
    default_args=default_args,  # Correction de la clé
    description='A DAG for pollution',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
)

# Define the tasks
first_extract_task = PythonOperator(  # Renommage pour éviter les conflits
    task_id='first_extract',
    python_callable=first_extract,
    dag=dag,
)

transform_data_task = PythonOperator(  # Renommage pour éviter les conflits
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

save_cleaned_data_task = PythonOperator(  # Renommage pour éviter les conflits
    task_id='save_cleaned_data',
    python_callable=save_cleaned_data,
    dag=dag,
)

# Set the task dependencies
first_extract_task >> transform_data_task >> save_cleaned_data_task
