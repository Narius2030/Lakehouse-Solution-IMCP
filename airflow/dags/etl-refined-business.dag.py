from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label
from airflow.utils.task_group import TaskGroup
from airflow.operators.python_operator import PythonOperator #type: ignore
from airflow.operators.dummy import DummyOperator #type: ignore
from load_refined import load_refined_data #type: ignore
from load_business_data import load_encoded_data, load_image_storage #type: ignore


# DAGs
with DAG(
    'IMCP_Refined_Business_Integration',
    schedule_interval='0 23 3 * *',
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
        
    # Silver process
    silver_data = PythonOperator(
        task_id = 'refine_raw_data',
        python_callable = load_refined_data,
        trigger_rule='one_success',
        dag = dag
    )
    
    
    # Gold process
    with TaskGroup("extract_image_features", tooltip="Tasks for image feature extraction") as gold:
        gold_data = PythonOperator(
            task_id = 'extract_feature',
            python_callable = load_encoded_data,
            do_xcom_push=True,
            dag = dag
        )
        upload_features = PythonOperator(
            task_id = 'upload_s3_feature',
            python_callable = load_image_storage,
            dag = dag
        )
        gold_data >> upload_features
    
    # End pipeline
    end = DummyOperator(task_id="end")



# pipeline
start >> Label("refine data") >> silver_data >> Label("extract feature") >> gold >> end

    