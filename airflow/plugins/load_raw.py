import sys
sys.path.append('/opt/airflow')

import pandas as pd
import warnings
import os
import requests
import cv2
import time
import polars as pl
import io
import pytz
from tqdm import tqdm
from datetime import timezone
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.operators.storage import MinioStorageOperator
from utils.images.yolov8_encoder import YOLOFeatureExtractor


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor(f'{settings.WORKING_DIRECTORY}/utils/images/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def dowload_raw_data(bucket_name, file_path, parquet_engine):
    datasets = {}
    file_name = os.path.basename(file_path)
    try:
        minio_operator.download_file(bucket_name, file_path, f'{settings.RAW_DATA_PATH}/{file_name}')
        df = pd.read_parquet(f'{settings.RAW_DATA_PATH}/{file_name}', parquet_engine)
        df['created_time'] = pd.to_datetime('now')
        df['publisher'] = 'HuggingFace'
        df['howpublished'] = 'https://huggingface.co/datasets/laion/220k-GPT4Vision-captions-from-LIVIS'
        datasets = df.to_dict('records')
    except Exception as ex:
        raise FileNotFoundError(str(ex))
    finally:
        os.remove(f'{settings.RAW_DATA_PATH}/{file_name}')
    return datasets


def upload_image(image_matrix, image_name, bucket_name, file_path):
    _, encoded_image = cv2.imencode('.jpg', image_matrix)
    image_bytes = io.BytesIO(encoded_image)
    minio_operator.upload_object_bytes(image_bytes, bucket_name, f'{file_path}/{image_name}', "image/jpeg")


def get_latest_time(source_table:str):
    latest_time = mongo_operator.find_latest_time('bronze', source_table)
    return latest_time


def check_for_new_parquet_files(params):
    while True:
        objects = minio_operator.get_list_files(params['bucket_name'], params['file_path'])
        print(objects)
        latest_time = get_latest_time('parquet')
        new_files = []
        for obj in objects:
            print('HERE ', obj.object_name)
            if obj.last_modified != None:
                obj_last_modified = obj.last_modified.astimezone(pytz.timezone('Asia/Ho_Chi_Minh'))
                latest_time = latest_time.astimezone(pytz.timezone('Asia/Ho_Chi_Minh'))
                if (obj_last_modified > latest_time):  # So sánh với thời gian xử lý cuối cùng
                    new_files.append(obj.object_name)
        if new_files != []:
            break
        time.sleep(1)

    print(new_files)
    return new_files


def load_raw_parquets(**kwargs):
    start_time = pd.to_datetime('now')
    affected_rows = 0
    # new_files = check_for_new_parquet_files()
    ti = kwargs['ti']
    file_pathes = ti.xcom_pull(task_ids="check_new_parquets")
    for file_path in file_pathes:
        datasets = dowload_raw_data(kwargs['params']['bucket_name'], file_path, kwargs['params']['engine'])
        try:
            warnings.warn("There is no documents in collection --> INGEST ALL")
            # If there is empty collection -> insert all
            affected_rows = mongo_operator.insert_batches('raw', datasets)
            # Write logs
            mongo_operator.write_log('parquet', layer='bronze', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        except Exception as ex:
            mongo_operator.write_log('parquet', layer='bronze', start_time=start_time, status="ERROR", error_message=str(ex), action="insert", affected_rows=affected_rows)
            raise Exception(str(ex))
  
  
def load_raw_user_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = get_latest_time('user')
    print("Latest time: ", latest_time)
    try:
        for batch in mongo_operator.data_generator('user_data'):
            data = list(batch)
            df = pl.DataFrame(data, infer_schema_length=1000)
            df = df.filter(pl.col('created_time') >= latest_time)
            print("Total rows of batch", df.shape)
            if df.shape[0]==0:
                continue
            df = df.with_columns(
                pl.lit('android').alias('publisher'),
                pl.lit(f'{settings.MINIO_URL}').alias('howpublished'),
                pl.col('manual_caption').alias('caption'),
                pl.lit('').alias('short_caption')
            ).drop(['predicted_caption','image_shape','manual_caption'])
            # Load to mongodb
            data = df.to_dicts()
            mongo_operator.insert_batches('raw', data)
            affected_rows += len(data)
            print('SUCCESS with', len(data))
        # Write logs
        mongo_operator.write_log('user', layer='bronze', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
    except Exception as exc:
        aggregate = [{'created_time': pd.to_datetime('now')}, {'$project': {'_id': 1}}]
        data = mongo_operator.find_data_with_aggregate('raw', aggregate)
        affected_rows = len(data)
        # Write logs
        mongo_operator.write_log('user', layer='bronze', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))
      

def load_raw_image(params):
    latest_time = get_latest_time('image')
    start_time = pd.to_datetime('now')
    affected_rows = 0
    try:
        for batch in mongo_operator.data_generator('raw', limit=10000):
            df = pl.DataFrame(batch, infer_schema_length=1000).filter(pl.col('created_time') >= latest_time)
            batch_data = df.to_dicts()
            for doc in tqdm(batch_data):
                image_url = doc['url']
                image_name = doc['url'][-16:]
                try:
                    image_repsonse = requests.get(image_url, timeout=1)
                    image_rgb = yolo_extractor.cv2_read_image(image_repsonse.content)
                    upload_image(image_rgb, image_name, params['bucket_name'], params['file_image_path'])
                except Exception:
                    for attempt in range(0, 2):
                        try:
                            image_repsonse = requests.get(image_url, timeout=1)
                            image_rgb = yolo_extractor.cv2_read_image(image_repsonse.content)
                            upload_image(image_rgb, image_name, params['bucket_name'], params['file_image_path'])
                            break  # Thành công, thoát khỏi vòng lặp thử lại
                        except Exception as e:
                            print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {str(e)}")
                            time.sleep(2)  # Chờ đợi trước khi thử lại
                affected_rows += 1
            print('SUCCESS with', len(batch_data))
        mongo_operator.write_log('image', layer='bronze', start_time=start_time, status="SUCCESS", action="upload", affected_rows=affected_rows)
    except Exception as exc:
        # Write logs
        mongo_operator.write_log('image', layer='bronze', start_time=start_time, status="ERROR", error_message=str(exc), action="upload", affected_rows=affected_rows)
        raise Exception(str(exc))
    
        

if __name__=='__main__':
    params = {
        'bucket_name': 'mlflow',
        'file_path': '/raw_data/lvis_caption_url.parquet',
        'engine': 'pyarrow'
    }
    load_raw_parquets(params)
    # load_raw_user_data()
    
    params = {
        'bucket_name': 'mlflow',
        'file_image_path': '/raw_data/raw_images',
    }
    # load_raw_image(params)