import sys
sys.path.append('/opt/airflow')

import os
from tqdm import tqdm
import time
import requests
import polars as pl
import pandas as pd
from datetime import datetime
from core.config import get_settings
from utils.images.yolov8_encoder import YOLOFeatureExtractor
from utils.operators.storage import MinioStorageOperator
from utils.operators.database import MongoDBOperator


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor(f'{settings.WORKING_DIRECTORY}/utils/images/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def get_latest_time(source_table:str):
    latest_time = mongo_operator.find_latest_time('gold', source_table)
    return latest_time


def encode_image(image_url, image_bytes, timestamp):
    features = {}
    image_rgb = yolo_extractor.cv2_read_image(image_bytes)
    # extract feature by yolov8
    transformed_image = yolo_extractor.preprocess_image(image_rgb)
    feature_matrix = yolo_extractor.extract_features(transformed_image)
    features[image_url] = feature_matrix.tolist()
    
    # read existing file and update key-value pairs with new values
    temp = yolo_extractor.load_feature(settings.EXTRACT_FEATURE_PATH, f"extracted_features_{timestamp}.pkl")
    if temp != None:
        features.update(temp)
    # write updated data into pkl again
    yolo_extractor.save_feature(features, settings.EXTRACT_FEATURE_PATH, f"extracted_features_{timestamp}.pkl")


def loop_batch_images(datasets, timestamp):
    
    for doc in tqdm(datasets):
        image_url = doc['url']
        try:
            image_repsonse = requests.get(image_url, timeout=1)
            encode_image(image_url, image_repsonse.content, timestamp)
        except Exception:
            for attempt in range(0, 2):
                try:
                    image_repsonse = requests.get(image_url, timeout=1)
                    encode_image(image_url, image_repsonse.content, timestamp)
                    break  # Thành công, thoát khỏi vòng lặp thử lại
                except Exception as e:
                    print(f"Tải lại dữ liệu từ {doc['url']} (lần {attempt+1}/{2}): {e}")
                    time.sleep(2)  # Chờ đợi trước khi thử lại


def load_encoded_data():
    start_time = pd.to_datetime('now')
    latest_time = get_latest_time('image_feature')
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    try:
        for batch in mongo_operator.data_generator('refined', limit=20):
            data = list(batch)
            df = pl.DataFrame(data, infer_schema_length=1000)
            df = df.filter(pl.col('created_time') >= latest_time)
            if df.shape[0]==0:
                continue
            print("Total rows of batch", df.shape)
            loop_batch_images(data, timestamp)
            print('SUCCESS with', len(data))
        # write logs
        mongo_operator.write_log('image_feature', layer='gold', start_time=start_time, status="SUCCESS", action="upload", affected_rows=len(data))
    except Exception as exc:
        mongo_operator.write_log('image_feature', layer='gold', start_time=start_time, status="ERROR", error_message=str(exc), action="upload", affected_rows=0)
        #Raise error
        raise Exception(str(exc))
    
    return f'extracted_features_{timestamp}.pkl'
     
                
def load_image_storage(**kwargs):
    ti = kwargs['ti']
    file_name = ti.xcom_pull(task_ids="extract_image_features.extract_feature")
    print(file_name)
    try:
        minio_operator.upload_file('mlflow', f'/features/{file_name}', f'{settings.EXTRACT_FEATURE_PATH}/{file_name}')
    except:
        raise Exception('Upload extracted feature file failed!')
    finally:
        os.remove(f'{settings.EXTRACT_FEATURE_PATH}/{file_name}')
            
            
if __name__=='__main__':
    # load_encoded_data()
    load_image_storage()
                 