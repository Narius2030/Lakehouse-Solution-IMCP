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
from utils.operators.storage import MinioStorageOperator
import logging

def check_collection_result(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids=f"validate_configs.check_collection_existence")
    logging.info(f"XCom result: {result}")
    # result will be a tuple with count value
    count = result[0] if result else 0
    if count != 4:  # Collection doesn't exist
        raise Exception("Collections do not exist")
    logging.info(f"Collections exist")
    return {
        "status": "success",
        "message": "Collections exist",
        "count": count
    }

def check_bucket_result():
    minio_operator = MinioStorageOperator(
        endpoint=Variable.get("MINIO_ENDPOINT", default_var="160.191.244.13:9000"),
        access_key=Variable.get("MINIO_ACCESS_KEY", default_var="minio"),
        secret_key=Variable.get("MINIO_SECRET_KEY", default_var="minio123"),
        secure=False
    )
    
    bucket_name = Variable.get("LAKEHOUSE_BUCKET", default_var="lakehouse")
    if not minio_operator.is_bucket_exists(bucket_name):
        raise Exception(f"Bucket {bucket_name} does not exist")
    logging.info(f"Bucket {bucket_name} exists")
    return {
        "status": "success",
        "message": f"Bucket {bucket_name} exists",
        "bucket_name": bucket_name
    }


# DAGs
with DAG(
    'IMCP_Augment_Encode',
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
    # Start and End tasks
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")
    
    # Validate Connection
    with TaskGroup("validate_configs", tooltip="Tasks for validations before running") as validate_connections:
        # skip_task = DummyOperator(task_id="skip_task")
        
        count_existing_collection = TrinoOperator(
            task_id="check_collection_existence",
            trino_conn_id="trino_conn",
            sql=f"""
                SELECT COUNT(*) as count
                FROM mongodb.information_schema.tables 
                WHERE table_schema = 'imcp' 
                AND table_name IN('audit','raw','refined','featured');
            """,
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
    
    # Gold process
    encode_data = PythonOperator(
        task_id = 'encode_data',
        python_callable = load_encoded_data,
        trigger_rule='all_success',
        dag = dag
    )



# pipeline
start >> validate_connections >> Label("augment") >> augment_data >> Label("encode") >> encode_data >> end