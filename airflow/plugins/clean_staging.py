import sys
sys.path.append('/opt/airflow')

import logging
from utils.setting import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.storage import MinioStorageOperator
from airflow.models.variable import Variable                            #type: ignore


settings = get_settings()
mongo_opt = MongoDBOperator('imcp', settings.DATABASE_URL)


def clean_null_data(params):
    try:
        affected_rows = mongo_opt.delete_null_from_latest(params["table_name"])
        logging.info(f"DELETED {affected_rows} ROWS IN COLLECTION REFINED")
    except Exception as ex:
        raise Exception(f"Error in deleteing - {str(ex)}")

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