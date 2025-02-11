import sys
sys.path.append('./airflow')

import pandas as pd
import polars as pl
from core.config import get_settings
from utils.operators.mongodb import MongoDBOperator
from utils.operators.trinodb import SQLOperators
from utils.operators.storage import MinioStorageOperator
from utils.pre_proccess import scaling_data, tokenize_vietnamese


settings = get_settings()
mongo_operator = MongoDBOperator('imcp', settings.DATABASE_URL)
sql_opt = SQLOperators('imcp', settings)
minio_operator = MinioStorageOperator(endpoint=f'{settings.MINIO_HOST}:{settings.MINIO_PORT}',
                                    access_key=settings.MINIO_USER,
                                    secret_key=settings.MINIO_PASSWD)


def load_refined_data():
    start_time = pd.to_datetime('now')
    affected_rows = 0
    # latest_time = sql_opt.get_latest_fetching_time('silver', 'augmented_metadata')
    try:
        for batch in sql_opt.data_generator('raw'):
            data = list(batch)
            df = pl.DataFrame(data)
            # df = df.filter(pl.col('created_time') >= latest_time)
            cleaned_df = df.with_columns(
                caption=pl.col("caption").str.to_lowercase(),
                # caption_tokens=pl.col("caption").map_elements(lambda caption: tokenize_vietnamese(caption), return_dtype=pl.String()),
                s3_url=pl.format("{}/augmented/images/{}", pl.lit(settings.MINIO_URL), pl.col("original_url").str.extract(r".*/(.*)").str.slice(-16, None)).alias("s3_url")
            )
            refined_df = scaling_data(cleaned_df, ['original_url', 's3_url', 'caption', 'caption_tokens', 'word_count'])
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