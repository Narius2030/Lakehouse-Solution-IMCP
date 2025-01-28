import sys
sys.path.append('/opt/airflow')

import pandas as pd
import polars as pl
from core.config import get_settings
from utils.operators.database import MongoDBOperator
from utils.operators.storage import MinioStorageOperator
from utils.pre_proccess import tokenize, scaling_data, clean_text
from utils.images.yolov8_encoder import YOLOFeatureExtractor


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
yolo_extractor = YOLOFeatureExtractor(f'{settings.WORKING_DIRECTORY}/utils/images/model/yolov8n.pt')
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)



def get_latest_time(source_table:str):
    latest_time = mongo_operator.find_latest_time('silver', source_table)
    return latest_time


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = get_latest_time('refined')
    try:
        for batch in mongo_operator.data_generator('raw', limit=50000):
            data = list(batch)
            df = pl.DataFrame(data).drop('_id')
            df = df.filter(pl.col('created_time') >= latest_time)
            lowered_df = df.with_columns(
                *[pl.col(col).str.to_lowercase().alias(col) for col in ['caption','short_caption']]
            )
            cleaned_df = lowered_df.with_columns(
                *[pl.col(col).map_elements(lambda x: clean_text(x), return_dtype=pl.String).alias(col) for col in ['caption','short_caption']],
                pl.format("{}/raw_data/raw_images/{}", pl.lit(settings.MINIO_URL), pl.col("url").str.extract(r".*/(.*)").str.slice(-16, None)).alias("s3_url")
            )
            tokenized_df = cleaned_df.with_columns(
                *[ pl.col(col).map_elements(lambda x: tokenize(x), return_dtype=pl.List(pl.String)).alias(f'{col}_tokens') for col in ['caption','short_caption']]
            )
            refined_df = scaling_data(tokenized_df, ['url', 's3_url', 'caption', 'short_caption', 'caption_tokens', 'short_caption_tokens', 'publisher', 'created_time'])
            data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', data)
            
            affected_rows += len(data)
            print('SUCCESS with', len(data))
        # Write logs
        mongo_operator.write_log('refined', layer='silver', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
    except Exception as exc:
        aggregate = [{'$sort': {'created_time': -1}}, {'$project': {'_id': 1}}]
        data = mongo_operator.find_data_with_aggregate('refined', aggregate)
        affected_rows = len(data)
        # Write logs
        mongo_operator.write_log('refined', layer='silver', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))
            

if __name__=='__main__':
    load_refined_data()