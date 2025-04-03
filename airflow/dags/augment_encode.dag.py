from datetime import timedelta
from airflow import DAG                                                                                 #type: ignore
from airflow.utils.dates import days_ago                                                                #type: ignore
from airflow.utils.edgemodifier import Label                                                            #type: ignore
from airflow.utils.task_group import TaskGroup                                                          #type: ignore  
from airflow.operators.python_operator import PythonOperator                                            #type: ignore
from airflow.operators.dummy import DummyOperator                                                       #type: ignore
from airflow.models.variable import Variable                                                            #type: ignore
from trino_operator import TrinoOperator                                                                #type: ignore
from load_augmented import load_refined_data                                                            #type: ignore
from load_encoded import load_encoded_data                                                              #type: ignore
from clean_staging import clean_null_data, check_collection_result, check_bucket_result                 #type: ignore


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
    
    # Validate Connection
    with TaskGroup("validate_configs", tooltip="Tasks for validations before running") as validate_connections:
        count_existing_collection = TrinoOperator(
            task_id="check_collection_existence",
            trino_conn_id="trino_conn",
            sql="./sql/count_existing_collections.sql",
            do_xcom_push=True
        )
        check_existing_collection = PythonOperator(
            task_id='check_existing_collection',
            python_callable=check_collection_result,
            provide_context=True,
            dag = dag
        )
        count_existing_collection >> check_existing_collection
        
        check_bucket = PythonOperator(
            task_id='check_bucket',
            python_callable=check_bucket_result,
            dag = dag
        )
    
    # Silver process
    with TaskGroup("data_augmentation", tooltip="Tasks for augmenting images") as data_augmentation:
        augment_data = PythonOperator(
            task_id = 'augment_data',
            params = {
                "bucket_name": Variable.get("LAKEHOUSE_BUCKET", default_var="lakehouse"),
                "file_image_path": Variable.get("AUGMENTED_IMAGE_PATH", default_var="imcp/augmented-data/images")
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
        python_callable = load_encoded_data,
        trigger_rule='all_success',
        dag = dag
    )



# pipeline
start >> validate_connections >> Label("augment") >> data_augmentation >> Label("encode") >> encode_data >> end