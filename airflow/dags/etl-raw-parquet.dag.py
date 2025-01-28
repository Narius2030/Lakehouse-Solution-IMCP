from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_raw import load_raw_parquets, check_for_new_parquet_files #type: ignore



# DAGs
with DAG(
    'IMCP_Raw_Data_Parquet_Integration',
    schedule_interval='0 23 * * *',
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
    
    check_new_parquets = PythonOperator(
        task_id = 'check_new_parquets',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_path': Variable.get('raw_data_path')
        },
        python_callable = check_for_new_parquet_files,
        do_xcom_push=True,
        dag = dag
    )
    
    bronze_data = PythonOperator(
        task_id = 'ingest_raw_parquet_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'engine': 'pyarrow'
        },
        python_callable = load_raw_parquets,
        dag = dag
    )
    
    # End pipeline
    end = DummyOperator(task_id="end")
    
start >> check_new_parquets >> bronze_data >> end