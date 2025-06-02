from datetime import timedelta
from airflow import DAG                                                                                 #type: ignore
from airflow.utils.dates import days_ago                                                                #type: ignore
from airflow.utils.edgemodifier import Label                                                            #type: ignore
from airflow.utils.task_group import TaskGroup                                                          #type: ignore  
from airflow.operators.python_operator import PythonOperator                                            #type: ignore
from airflow.operators.dummy import DummyOperator                                                       #type: ignore
from airflow.models.variable import Variable                                                            #type: ignore
from load_augmented import load_refined_data                                                            #type: ignore
from load_encoded import load_encoded_data                                                              #type: ignore
from clean_staging import clean_null_data,                                                              #type: ignore


# DAGs
with DAG(
    'IMCP_Augment_Encode',
    schedule_interval='0 23 3 * *',
    default_args={
        'start_date': days_ago(1),
        'email_on_failure': True,
        'email_on_success': True,
        'email_on_retry': True,
        'email': ['nhanbui15122003@gmail.com', 'dtptrieuphidtp@gmail.com', '159.thiennhan@gmail.com'],
        'execution_timeout': timedelta(minutes=120)
    },
    catchup=False
) as dag:
    # Start and End tasks
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    # Silver process
    with TaskGroup("data_augmentation", tooltip="Tasks for augmenting images") as data_augmentation:
        augment_data = PythonOperator(
            task_id = 'augment_data',
            params = {
                "bucket_name": Variable.get("S3_BUCKET", default_var="lakehouse"),
                "storage_type": Variable.get("STORAGE_TYPE", default_var="minio"),
                "layer_name": Variable.get("REFINED_LAYER", default_var="refined")
            },
            python_callable = load_refined_data,
            trigger_rule='all_success',
            dag = dag
        )
        cleaning_augmented_data = PythonOperator(
            task_id = 'clean_null_data',
            params = {
                "table_name": Variable.get("CLEANED_TABLE_NAME", default_var="augmented_metadata")
            },
            python_callable=clean_null_data,
            trigger_rule='all_success',
            dag=dag
        )
        augment_data >> cleaning_augmented_data
    
    # Gold process
    encode_data = PythonOperator(
        task_id = 'encode_data',
        params = {
            "bucket_name": Variable.get("S3_BUCKET", default_var="lakehouse"),
            "storage_type": Variable.get("STORAGE_TYPE", default_var="minio"),
            "layer_name": Variable.get("FEATURED_LAYER", default_var="featured")
        },
        python_callable = load_encoded_data,
        trigger_rule='all_success',
        dag = dag
    )

# pipeline
start >> Label("augment") >> data_augmentation >> Label("encode") >> encode_data >> end