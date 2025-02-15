import sys
sys.path.append('./airflow')

import pandas as pd
import polars as pl
from core.config import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.storage import MinioStorageOperator
from utils.pre_proccess import scaling_data, clean_caption


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    try:
        for batch in sql_opt.data_generator('raw', latest_time=latest_time):
            data = list(batch)
            df = pl.DataFrame(data)
            
            # for key, val in data.items():
            #     ## TODO: augment images
            #     ## TODO: upload images to MinIO
            #     ## TODO: insert metadata
            #     break
            
            cleaned_df = clean_caption(df, settings.MINIO_URL)
            refined_df = scaling_data(cleaned_df, ['original_url', 's3_url', 'caption', 'caption_tokens', 'word_count', 'created_time'])
            # insert data batch
            data = refined_df.to_dicts()
            mongo_operator.insert_batches('refined', data)
            affected_rows += len(data)
            print('SUCCESS with', len(data))
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="SUCCESS", action="insert", affected_rows=affected_rows)
        
    except Exception as exc:
        # Write logs
        sql_opt.write_log('augmented_metadata', layer='silver', start_time=start_time, status="ERROR", error_message=str(exc), action="insert", affected_rows=affected_rows)
        raise Exception(str(exc))
            

if __name__=='__main__':
    load_refined_data()