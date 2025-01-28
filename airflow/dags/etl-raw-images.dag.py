from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_raw import load_raw_image #type: ignore



# DAGs
with DAG(
    'IMCP_Raw_Image_Integration',
    schedule_interval='0 23 1 * *',
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
    
    bronze_image_data = PythonOperator(
        task_id = 'ingest_raw_image_data',
        params = {
            'bucket_name': Variable.get('bucket_name'),
            'file_image_path': Variable.get('raw_image_path')
        },
        python_callable = load_raw_image,
        trigger_rule='one_success',
        dag = dag
    )
    
    # End pipeline
    end = DummyOperator(task_id="end")
    
start >> bronze_image_data >> end