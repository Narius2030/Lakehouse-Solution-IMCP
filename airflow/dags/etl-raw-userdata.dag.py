from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_raw import load_raw_user_data #type: ignore



# DAGs
with DAG(
    'IMCP_Raw_User_Data_Integration',
    schedule_interval='0 23 * * 1',
    default_args={
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': True,
        'email': ['nhanbui15122003@gmail.com', 'dtptrieuphidtp@gmail.com', '159.thiennhan@gmail.com']
    },
    catchup=False
) as dag:
    # Start pipeline
    start = DummyOperator(task_id="start")
    
    bronze_user_data = PythonOperator(
        task_id = 'ingest_raw_user_data',
        python_callable = load_raw_user_data,
        dag = dag
    )
    
    # End pipeline
    end = DummyOperator(task_id="end")
    
start >> bronze_user_data >> end